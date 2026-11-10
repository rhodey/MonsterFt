use monsterft::{
    error::Code,
    log::{LogEntry, SqliteLog},
    storage::{self, ProcRole, ProtocolStore, Receipt},
};
use rusqlite::{Connection, params};
use std::path::PathBuf;

struct Locals {
    _temp: tempfile::TempDir,
    base: PathBuf,
    stores: [ProtocolStore; 3],
    apply: Connection,
}

impl Locals {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let base = temp.path().join("db");
        let mut stores = ProcRole::ALL.map(|role| ProtocolStore::new(role, &base, 2).unwrap());
        for store in &mut stores {
            store.open(&[1; 16]).unwrap();
        }
        let apply = storage::open_apply_database(&base, &[1; 16]).unwrap();
        Self {
            _temp: temp,
            base,
            stores,
            apply,
        }
    }

    fn append_all(&mut self, term: i64, entry: &[u8]) -> i64 {
        let mut seq = -1;
        for store in &mut self.stores {
            store
                .db()
                .unwrap()
                .execute_batch("BEGIN IMMEDIATE")
                .unwrap();
            seq = store
                .log
                .append(
                    &LogEntry {
                        term,
                        entry: entry.to_vec(),
                    },
                    None,
                )
                .unwrap();
            store.db().unwrap().execute_batch("COMMIT").unwrap();
        }
        seq
    }

    fn receipt(&self, seq: i64, term: i64, entry: &[u8], digest: [u8; 16]) -> Receipt {
        let receipt = Receipt {
            seq,
            entry_hash: Some(storage::digest_entry(term, entry)),
            local_digest: Some(digest),
        };
        self.apply.execute_batch("BEGIN IMMEDIATE").unwrap();
        storage::write_receipt(&self.apply, &receipt).unwrap();
        self.apply.execute_batch("COMMIT").unwrap();
        receipt
    }
}

#[test]
fn paths_and_full_sync_are_preserved() {
    let locals = Locals::new();
    let paths = storage::database_paths(&locals.base).unwrap();
    assert!(paths.proc_a.ends_with("db1a"));
    assert!(paths.proc_b.ends_with("db1b"));
    assert!(paths.proc_c.ends_with("db1c"));
    assert!(paths.apply.ends_with("db2"));
    for db in locals
        .stores
        .iter()
        .map(|s| s.db().unwrap())
        .chain(std::iter::once(&locals.apply))
    {
        assert_eq!(
            db.query_row("PRAGMA journal_mode", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "wal"
        );
        assert_eq!(
            db.query_row("PRAGMA synchronous", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
    }
    assert_eq!(
        storage::database_paths(":memory:").unwrap_err().code,
        Some(Code::ArgumentIllegal)
    );
}

#[test]
fn log_batches_require_owner_transaction_and_rollback_without_partial_rows() {
    let temp = tempfile::tempdir().unwrap();
    let mut log = SqliteLog::new(temp.path().join("log"), 2).unwrap();
    log.open(|_| Ok(())).unwrap();
    let pair = vec![
        LogEntry {
            term: 1,
            entry: b"sync".to_vec(),
        },
        LogEntry {
            term: 1,
            entry: b"cmd".to_vec(),
        },
    ];
    assert_eq!(
        log.append_batch(&pair, None).unwrap_err().code,
        Some(Code::RaftIllegal)
    );
    log.db().unwrap().execute_batch("CREATE TRIGGER fail_second BEFORE INSERT ON raft_log WHEN NEW.seq=1 BEGIN SELECT RAISE(ABORT,'injected'); END; BEGIN IMMEDIATE").unwrap();
    assert!(log.append_batch(&pair, None).is_err());
    log.db()
        .unwrap()
        .execute_batch("ROLLBACK; DROP TRIGGER fail_second")
        .unwrap();
    assert_eq!(
        log.db()
            .unwrap()
            .query_row("SELECT count(*) FROM raft_log", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(log.seq, -1);
    log.db().unwrap().execute_batch("BEGIN IMMEDIATE").unwrap();
    assert_eq!(log.append_batch(&pair, None).unwrap(), 0);
    log.db().unwrap().execute_batch("COMMIT").unwrap();
    assert_eq!(log.seq, 1);
    assert_eq!(log.sizes(0, 2).unwrap(), vec![4, 3]);
    assert_eq!(
        log.iter(0)
            .unwrap()
            .collect::<monsterft::error::Result<Vec<_>>>()
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn log_reads_validate_relevant_rows_and_gaps() {
    let mut locals = Locals::new();
    locals.append_all(1, b"0");
    locals.append_all(1, b"1");
    locals.append_all(1, b"2");
    let log = &mut locals.stores[0].log;
    log.db()
        .unwrap()
        .execute("DELETE FROM raft_log WHERE seq=1", [])
        .unwrap();
    assert_eq!(log.range(0, 2, 3).unwrap_err().code, Some(Code::LogCorrupt));
    log.db()
        .unwrap()
        .execute("UPDATE raft_log SET term=-1 WHERE seq=2", [])
        .unwrap();
    assert_eq!(log.read_head().unwrap_err().code, Some(Code::LogCorrupt));
}

#[test]
fn replacement_generation_stops_old_owners_and_does_not_change_state_digest() {
    let locals = Locals::new();
    let old = &locals.stores[0];
    let digest = storage::state_digest(&storage::read_state(old.db().unwrap()).unwrap()).unwrap();
    let mut replacement = ProtocolStore::new(ProcRole::A, &locals.base, 2).unwrap();
    replacement.open(&[2; 16]).unwrap();
    assert_eq!(
        old.check_generation().unwrap_err().code,
        Some(Code::GenerationDiff)
    );
    assert_eq!(
        storage::state_digest(&storage::read_state(replacement.db().unwrap()).unwrap()).unwrap(),
        digest
    );
    let replacement_apply = storage::open_apply_database(&locals.base, &[2; 16]).unwrap();
    assert_eq!(
        storage::check_generation(&locals.apply, "monsterft_meta_apply", &[1; 16])
            .unwrap_err()
            .code,
        Some(Code::GenerationDiff)
    );
    storage::check_generation(&replacement_apply, "monsterft_meta_apply", &[2; 16]).unwrap();
}

#[test]
fn partial_pristine_initialization_resumes_but_missing_established_local_does_not() {
    let temp = tempfile::tempdir().unwrap();
    let base = temp.path().join("db");
    let mut first = ProtocolStore::new(ProcRole::A, &base, 2).unwrap();
    first.open(&[1; 16]).unwrap();
    storage::check_locals(&storage::database_paths(&base).unwrap()).unwrap();
    first
        .log
        .append(
            &LogEntry {
                term: 1,
                entry: vec![],
            },
            None,
        )
        .unwrap();
    let error = storage::check_locals(&storage::database_paths(&base).unwrap()).unwrap_err();
    assert_eq!(error.code, Some(Code::MonsterCorrupt));
    assert!(error.message.contains("established"));
}

#[test]
fn every_proc_captures_majority_and_outlier_adopts_without_changing_generation() {
    let mut locals = Locals::new();
    locals.append_all(1, b"original");
    locals.stores[0]
        .db()
        .unwrap()
        .execute(
            "UPDATE raft_log SET entry=? WHERE seq=0",
            [b"outlier".as_slice()],
        )
        .unwrap();
    let captures = locals
        .stores
        .iter_mut()
        .map(|store| store.capture().unwrap())
        .collect::<Vec<_>>();
    assert!(
        captures
            .iter()
            .all(|capture| capture.state == captures[0].state)
    );
    assert!(captures.iter().all(|capture| capture.donor == ProcRole::B));
    for store in &mut locals.stores {
        store.adopt().unwrap();
        store.check_generation().unwrap();
    }
    assert_eq!(
        locals.stores[0].log.head.as_deref(),
        Some(b"original".as_slice())
    );
}

#[test]
fn adoption_rechecks_the_other_witness_before_destructive_copy() {
    let mut locals = Locals::new();
    locals.append_all(1, b"original");
    locals.stores[0]
        .db()
        .unwrap()
        .execute(
            "UPDATE raft_log SET entry=? WHERE seq=0",
            [b"outlier".as_slice()],
        )
        .unwrap();
    locals.stores[0].capture().unwrap();
    locals.stores[2]
        .db()
        .unwrap()
        .execute(
            "UPDATE raft_log SET entry=? WHERE seq=0",
            [b"changed witness".as_slice()],
        )
        .unwrap();
    assert_eq!(
        locals.stores[0].adopt().unwrap_err().code,
        Some(Code::MonsterCorrupt)
    );
    let entry: Vec<u8> = locals.stores[0]
        .db()
        .unwrap()
        .query_row("SELECT entry FROM raft_log WHERE seq=0", [], |r| r.get(0))
        .unwrap();
    assert_eq!(entry, b"outlier");
    locals.stores[0]
        .db()
        .unwrap()
        .execute_batch("ROLLBACK")
        .unwrap();
}

#[test]
fn no_majority_is_never_automatically_adopted() {
    let mut locals = Locals::new();
    locals.append_all(1, b"original");
    for (index, store) in locals.stores.iter().enumerate() {
        store
            .db()
            .unwrap()
            .execute(
                "UPDATE raft_log SET entry=? WHERE seq=0",
                [vec![index as u8]],
            )
            .unwrap();
    }
    for store in &mut locals.stores {
        assert_eq!(
            store.capture().unwrap_err().code,
            Some(Code::MonsterCorrupt)
        );
    }
}

#[test]
fn committed_apply_receipt_imports_after_interrupted_proc_progress() {
    let mut locals = Locals::new();
    locals.append_all(1, b"setup");
    let receipt = locals.receipt(0, 1, b"setup", [3; 16]);
    for store in &mut locals.stores {
        store.capture().unwrap();
    }
    for store in &mut locals.stores {
        store.adopt().unwrap();
        let meta = store.reconcile().unwrap();
        assert_eq!(meta.applied_seq, 0);
        assert_eq!(meta.pending_cmd_seq, None);
        assert_eq!(meta.applied_local_digest, receipt.local_digest);
    }
    locals.append_all(1, b"command");
    let receipt = locals.receipt(1, 1, b"command", [4; 16]);
    for store in &mut locals.stores {
        store.capture().unwrap();
    }
    for store in &mut locals.stores {
        store.adopt().unwrap();
        let meta = store.reconcile().unwrap();
        assert_eq!(meta.applied_seq, 1);
        assert_eq!(meta.pending_cmd_seq, Some(1));
        assert_eq!(meta.applied_local_digest, receipt.local_digest);
    }
}

#[test]
fn receipt_reconciliation_rejects_skips_and_relevant_corrupt_entries() {
    let mut locals = Locals::new();
    locals.append_all(1, b"setup");
    locals.append_all(1, b"command");
    let skipped = Receipt {
        seq: 1,
        entry_hash: Some(storage::digest_entry(1, b"command")),
        local_digest: Some([7; 16]),
    };
    let db = locals.stores[0].db().unwrap();
    assert_eq!(
        storage::reconcile_receipt(db, &skipped).unwrap_err().code,
        Some(Code::MonsterCorrupt)
    );
    let setup = Receipt {
        seq: 0,
        entry_hash: Some(storage::digest_entry(1, b"setup")),
        local_digest: Some([7; 16]),
    };
    db.execute("UPDATE raft_log SET term=-1 WHERE seq=0", [])
        .unwrap();
    assert_eq!(
        storage::reconcile_receipt(db, &setup).unwrap_err().code,
        Some(Code::LogCorrupt)
    );
    db.execute(
        "UPDATE raft_log SET term=1,entry=? WHERE seq=0",
        [b"changed".as_slice()],
    )
    .unwrap();
    assert_eq!(
        storage::reconcile_receipt(db, &setup).unwrap_err().code,
        Some(Code::MonsterCorrupt)
    );
}

#[test]
fn receipt_cannot_go_backwards_or_be_written_outside_a_transaction() {
    let locals = Locals::new();
    let receipt = Receipt {
        seq: 0,
        entry_hash: Some([3; 16]),
        local_digest: Some([4; 16]),
    };
    assert_eq!(
        storage::write_receipt(&locals.apply, &receipt)
            .unwrap_err()
            .code,
        Some(Code::MonsterIllegal)
    );
    locals.apply.execute_batch("BEGIN IMMEDIATE").unwrap();
    storage::write_receipt(&locals.apply, &receipt).unwrap();
    locals.apply.execute_batch("COMMIT").unwrap();
    locals.apply.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert_eq!(
        storage::write_receipt(&locals.apply, &receipt)
            .unwrap_err()
            .code,
        Some(Code::SqliteError)
    );
    locals.apply.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn certification_clears_fences_and_repairs_outlier_offline() {
    let mut locals = Locals::new();
    locals.append_all(1, b"setup");
    let receipt = locals.receipt(0, 1, b"setup", [3; 16]);
    for store in &mut locals.stores {
        let db = store.db().unwrap();
        db.execute_batch("BEGIN IMMEDIATE").unwrap();
        storage::copy_apply_into(db, &receipt).unwrap();
        db.execute(
            "UPDATE monsterft_meta SET repair_state=10020 WHERE id=1",
            [],
        )
        .unwrap();
        db.execute_batch("COMMIT").unwrap();
    }
    locals.stores[0]
        .db()
        .unwrap()
        .execute(
            "UPDATE raft_log SET entry=? WHERE seq=0",
            [b"outlier".as_slice()],
        )
        .unwrap();
    for store in &mut locals.stores {
        store.close().unwrap();
    }
    locals.apply.close().unwrap();
    assert_eq!(monsterft::certify::certify(&locals.base, false).unwrap(), 0);
    let paths = storage::database_paths(&locals.base).unwrap();
    for role in ProcRole::ALL {
        storage::read_database(paths.proc(role), |db| {
            assert_eq!(storage::read_meta(db)?.repair_state, 0);
            assert_eq!(storage::read_state(db)?.head.unwrap().entry, b"setup");
            Ok(())
        })
        .unwrap();
    }
}

#[test]
fn metadata_and_hashes_reject_invalid_stored_values() {
    let locals = Locals::new();
    let db = locals.stores[0].db().unwrap();
    db.execute_batch("PRAGMA ignore_check_constraints=ON")
        .unwrap();
    db.execute(
        "UPDATE monsterft_meta SET generation=? WHERE id=1",
        [vec![0; 16]],
    )
    .unwrap();
    assert_eq!(
        storage::read_meta(db).unwrap_err().code,
        Some(Code::MonsterCorrupt)
    );
    db.execute("UPDATE monsterft_meta SET generation=?,applied_seq=0,applied_entry_hash=?,applied_local_digest=? WHERE id=1",params![vec![1u8;16],vec![3u8;15],vec![4u8;16]]).unwrap();
    assert_eq!(
        storage::read_meta(db).unwrap_err().code,
        Some(Code::MonsterCorrupt)
    );
}

#[test]
fn receipt_binary_serialization_is_exact_and_checked() {
    let receipt = Receipt {
        seq: 3,
        entry_hash: Some([1; 16]),
        local_digest: Some([2; 16]),
    };
    let encoded = monsterft::wire::encode_payload(&receipt).unwrap();
    let decoded: Receipt = monsterft::wire::decode_payload(&encoded).unwrap();
    assert_eq!(decoded, receipt);
    assert_eq!(encoded.iter().filter(|byte| **byte == 0xc4).count(), 2);
}
