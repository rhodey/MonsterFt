#[allow(dead_code)]
#[path = "../bench/config.rs"]
mod config;
#[allow(dead_code)]
#[path = "../bench/delay.rs"]
mod delay;
#[allow(dead_code)]
#[path = "../bench/workload.rs"]
mod workload;

use monsterft::protocol::{CommandResult, Outcome};

fn batch(ordinal: usize, size: usize) -> CommandResult {
    let first = (ordinal - 1) * size + 1;
    CommandResult {
        cmd_seq: (ordinal * 2 - 1) as i64,
        results: (first..first + size)
            .map(|n| Outcome::Fulfilled((n as u64).into()))
            .collect(),
    }
}

#[test]
fn validates_all_items_across_batch_and_phase_boundaries() {
    for size in [1, 128, 256, 512, 1024] {
        for ordinal in [1, 2, 65, 10_001] {
            workload::validate_batch(&batch(ordinal, size), ordinal, size).unwrap();
        }
    }
}

#[test]
fn rejects_missing_duplicated_reordered_or_rejected_items() {
    let original = batch(65, 128);
    let mut missing = original.clone();
    missing.results.pop();
    let mut duplicate = original.clone();
    duplicate.results[64] = duplicate.results[63].clone();
    let mut reordered = original.clone();
    reordered.results.swap(3, 4);
    let mut rejected = original.clone();
    rejected.results[7] = Outcome::Rejected(config::error("test rejection"));
    for result in [missing, duplicate, reordered, rejected, batch(64, 128)] {
        assert!(workload::validate_batch(&result, 65, 128).is_err());
    }
}

#[test]
fn benchmark_configuration_bounds_batch_sizes() {
    let file = tempfile::NamedTempFile::new().unwrap();
    for size in [0, 1, 128, 1024, 1025] {
        let value = serde_json::json!({
            "run_dir": file.path().parent().unwrap(), "base_port": 45000,
            "rtt_us": 0, "window": 32, "batch_size": size,
            "warmup_ms": 0, "measure_ms": 10,
        });
        std::fs::write(file.path(), serde_json::to_vec(&value).unwrap()).unwrap();
        assert_eq!(
            config::Config::read(file.path()).is_ok(),
            (1..=1024).contains(&size)
        );
    }
}
