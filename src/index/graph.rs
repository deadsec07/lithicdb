use crate::models::types::ClusterNode;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

#[derive(Debug, Clone)]
struct FrontierItem {
    score: f32,
    cluster_id: u32,
}

impl PartialEq for FrontierItem {
    fn eq(&self, other: &Self) -> bool {
        self.score.eq(&other.score)
    }
}

impl Eq for FrontierItem {}

impl PartialOrd for FrontierItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FrontierItem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(Ordering::Equal)
    }
}

pub fn centroid_score(query: &[f32], centroid: &[f32]) -> f32 {
    query.iter().zip(centroid.iter()).map(|(a, b)| a * b).sum()
}

pub fn top_n_centroids(
    clusters: &HashMap<u32, ClusterNode>,
    entry: Option<u32>,
    query: &[f32],
    entry_points: usize,
    ef_search: usize,
    probe_clusters: usize,
) -> Vec<u32> {
    let Some(entry_cluster) = entry else {
        return Vec::new();
    };

    let mut frontier = BinaryHeap::new();
    let mut visited = HashSet::new();
    let mut best = BinaryHeap::new();

    let mut seeds: Vec<(u32, f32)> = clusters
        .iter()
        .map(|(cluster_id, cluster)| (*cluster_id, centroid_score(query, &cluster.centroid)))
        .collect();
    seeds.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
    if seeds.is_empty() {
        if let Some(cluster) = clusters.get(&entry_cluster) {
            frontier.push(FrontierItem {
                score: centroid_score(query, &cluster.centroid),
                cluster_id: entry_cluster,
            });
        }
    } else {
        for (cluster_id, score) in seeds.into_iter().take(entry_points.max(1)) {
            frontier.push(FrontierItem { score, cluster_id });
        }
    }

    while let Some(item) = frontier.pop() {
        if !visited.insert(item.cluster_id) {
            continue;
        }
        best.push(item.clone());
        if visited.len() >= ef_search {
            break;
        }
        if let Some(cluster) = clusters.get(&item.cluster_id) {
            for neighbor in &cluster.neighbors {
                if visited.contains(neighbor) {
                    continue;
                }
                if let Some(next) = clusters.get(neighbor) {
                    frontier.push(FrontierItem {
                        score: centroid_score(query, &next.centroid),
                        cluster_id: *neighbor,
                    });
                }
            }
        }
    }

    let mut collected: Vec<_> = best.into_sorted_vec();
    collected.reverse();
    collected
        .into_iter()
        .take(probe_clusters.max(1))
        .map(|item| item.cluster_id)
        .collect()
}
