use crate::models::types::{ActiveSegmentManifest, GenerationCatalog};
use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct CollectionPaths {
    pub root: PathBuf,
    pub current_file: PathBuf,
    pub generations_file: PathBuf,
    pub wal_file: PathBuf,
}

#[derive(Debug, Clone)]
pub struct ActiveSegmentPaths {
    pub state_file: PathBuf,
    pub raw_vectors_file: PathBuf,
    pub quant_vectors_file: PathBuf,
}

impl CollectionPaths {
    pub fn new(base: impl AsRef<Path>, name: &str) -> Result<Self> {
        let root = base.as_ref().join(name);
        create_dir_all(&root)?;
        Ok(Self {
            current_file: root.join("CURRENT"),
            generations_file: root.join("GENERATIONS"),
            wal_file: root.join("wal.bin"),
            root,
        })
    }

    pub fn active_paths(&self, manifest: &ActiveSegmentManifest) -> ActiveSegmentPaths {
        ActiveSegmentPaths {
            state_file: self.root.join(&manifest.state_file),
            raw_vectors_file: self.root.join(&manifest.raw_vectors_file),
            quant_vectors_file: self.root.join(&manifest.quant_vectors_file),
        }
    }
}

pub fn default_active_manifest() -> ActiveSegmentManifest {
    ActiveSegmentManifest {
        generation: "base".to_string(),
        state_file: "state.bin".to_string(),
        raw_vectors_file: "vectors.f32".to_string(),
        quant_vectors_file: "vectors.q8".to_string(),
    }
}

pub fn default_generation_catalog() -> GenerationCatalog {
    let active = default_active_manifest();
    GenerationCatalog {
        write: active.clone(),
        readable: vec![active],
        retired: Vec::new(),
    }
}

pub fn generation_manifest(generation: impl Into<String>) -> ActiveSegmentManifest {
    let generation = generation.into();
    ActiveSegmentManifest {
        state_file: format!("state-{generation}.bin"),
        raw_vectors_file: format!("vectors-{generation}.f32"),
        quant_vectors_file: format!("vectors-{generation}.q8"),
        generation,
    }
}

pub fn open_append(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path)?)
}

pub fn open_rw(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)?)
}

pub fn append_f32_vector(file: &mut File, vector: &[f32]) -> Result<u64> {
    let offset = file.seek(SeekFrom::End(0))?;
    for value in vector {
        file.write_all(&value.to_le_bytes())?;
    }
    Ok(offset)
}

pub fn append_i8_vector(file: &mut File, vector: &[i8]) -> Result<u64> {
    let offset = file.seek(SeekFrom::End(0))?;
    let bytes: Vec<u8> = vector.iter().map(|v| *v as u8).collect();
    file.write_all(&bytes)?;
    Ok(offset)
}

pub fn read_f32_vector(file: &mut File, offset: u64, dim: usize) -> Result<Vec<f32>> {
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = vec![0u8; dim * 4];
    file.read_exact(&mut bytes)?;
    let mut vector = Vec::with_capacity(dim);
    for chunk in bytes.chunks_exact(4) {
        vector.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    Ok(vector)
}

pub fn read_i8_vector(file: &mut File, offset: u64, dim: usize) -> Result<Vec<i8>> {
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = vec![0u8; dim];
    file.read_exact(&mut bytes)?;
    Ok(bytes.into_iter().map(|b| b as i8).collect())
}

pub fn write_state<T: Serialize>(path: &Path, state: &T) -> Result<()> {
    let bytes = bincode::serialize(state).context("serialize snapshot")?;
    let tmp_path = path.with_extension("tmp");
    let mut file = File::create(&tmp_path).context("create temp state snapshot")?;
    file.write_all(&bytes).context("write temp state snapshot")?;
    file.sync_all().context("sync temp state snapshot")?;
    std::fs::rename(&tmp_path, path).context("rename temp state snapshot")?;
    Ok(())
}

pub fn read_state<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let bytes = std::fs::read(path).context("read state snapshot")?;
    let state = bincode::deserialize(&bytes).context("deserialize snapshot")?;
    Ok(state)
}

pub fn read_storage_manifest(path: &Path) -> Result<ActiveSegmentManifest> {
    if !path.exists() {
        return Ok(default_active_manifest());
    }
    read_state(path)
}

pub fn write_storage_manifest(path: &Path, manifest: &ActiveSegmentManifest) -> Result<()> {
    write_state(path, manifest)
}

pub fn read_generation_catalog(path: &Path) -> Result<GenerationCatalog> {
    if !path.exists() {
        return Ok(default_generation_catalog());
    }
    read_state(path)
}

pub fn write_generation_catalog(path: &Path, catalog: &GenerationCatalog) -> Result<()> {
    write_state(path, catalog)
}

pub fn append_log_entry<T: Serialize>(file: &mut File, entry: &T) -> Result<()> {
    let bytes = bincode::serialize(entry).context("serialize wal entry")?;
    let len = bytes.len() as u32;
    let checksum = checksum(&bytes);
    file.seek(SeekFrom::End(0))?;
    file.write_all(&len.to_le_bytes())?;
    file.write_all(&checksum.to_le_bytes())?;
    file.write_all(&bytes)?;
    file.sync_data()?;
    Ok(())
}

pub fn read_log_entries<T: DeserializeOwned>(path: &Path) -> Result<Vec<T>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut file = open_rw(path)?;
    file.seek(SeekFrom::Start(0))?;
    let mut entries = Vec::new();
    loop {
        let mut len_bytes = [0u8; 4];
        match file.read_exact(&mut len_bytes) {
            Ok(_) => {
                let len = u32::from_le_bytes(len_bytes) as usize;
                let mut checksum_bytes = [0u8; 8];
                file.read_exact(&mut checksum_bytes)?;
                let stored_checksum = u64::from_le_bytes(checksum_bytes);
                let mut payload = vec![0u8; len];
                file.read_exact(&mut payload)?;
                let actual_checksum = checksum(&payload);
                if stored_checksum != actual_checksum {
                    anyhow::bail!("wal checksum mismatch");
                }
                let entry = bincode::deserialize(&payload).context("deserialize wal entry")?;
                entries.push(entry);
            }
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err.into()),
        }
    }
    Ok(entries)
}

pub fn truncate_file(file: &mut File) -> Result<()> {
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.sync_all()?;
    Ok(())
}

pub fn file_len(path: &Path) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    Ok(std::fs::metadata(path)?.len())
}

fn checksum(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325u64, |acc, byte| {
        let mixed = acc ^ u64::from(*byte);
        mixed.wrapping_mul(0x100000001b3)
    })
}
