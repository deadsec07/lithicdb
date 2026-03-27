pub fn normalize(vector: &[f32]) -> Vec<f32> {
    let norm = vector.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm <= f32::EPSILON {
        return vector.to_vec();
    }
    vector.iter().map(|v| v / norm).collect()
}

pub fn quantize_normalized(vector: &[f32]) -> Vec<i8> {
    vector
        .iter()
        .map(|v| (v.clamp(-1.0, 1.0) * 127.0).round() as i8)
        .collect()
}

pub fn cosine_quantized(query_normalized: &[f32], encoded: &[i8]) -> f32 {
    query_normalized
        .iter()
        .zip(encoded.iter())
        .map(|(q, v)| q * (*v as f32 / 127.0))
        .sum()
}

pub fn cosine(query_normalized: &[f32], vector_normalized: &[f32]) -> f32 {
    query_normalized
        .iter()
        .zip(vector_normalized.iter())
        .map(|(a, b)| a * b)
        .sum()
}

