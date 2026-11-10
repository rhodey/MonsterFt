//! Offline certification of an operator-selected local database set.

use crate::{
    error::{Code, Error, Result},
    storage::{
        ProcRole, Receipt, copy_db1_into, database_paths, read_apply, read_state,
        reconcile_receipt, state_digest,
    },
};
use rusqlite::Connection;
use std::path::Path;

fn require_file(path: &Path, name: &str) -> Result<()> {
    if !std::fs::metadata(path).is_ok_and(|metadata| metadata.is_file()) {
        return Err(Error::new(
            Code::FsError,
            format!("certify {name} must be a normal file"),
        ));
    }
    Ok(())
}

fn clear_fence(db: &Connection, receipt: &Receipt, allow_unresolved: bool) -> Result<i64> {
    let meta = reconcile_receipt(db, receipt)?;
    if !allow_unresolved && meta.pending_cmd_seq.is_some() {
        return Err(Error::new(
            Code::ArgumentIllegal,
            "certify requires every CMD output to have an applied SYNC",
        ));
    }
    if db.execute("UPDATE monsterft_meta SET repair_state=0 WHERE id=1", [])? != 1 {
        return Err(Error::new(
            Code::SqliteError,
            "certify update count not one",
        ));
    }
    Ok(meta.applied_seq)
}

pub fn certify(base: impl AsRef<Path>, allow_unresolved: bool) -> Result<i64> {
    let paths = database_paths(base)?;
    require_file(&paths.apply, "apply")?;
    for role in ProcRole::ALL {
        require_file(paths.proc(role), role.as_str())?;
    }
    let mut databases = Vec::new();
    let result = (|| -> Result<i64> {
        // All owners must be stopped. Lock DB2 before the protocol copies because
        // application work is committed there before a Proc imports its receipt.
        for path in [&paths.apply, &paths.proc_a, &paths.proc_b, &paths.proc_c] {
            let db = Connection::open(path)?;
            databases.push(db);
            databases.last().expect("just pushed").execute_batch(
                "PRAGMA synchronous=FULL; PRAGMA busy_timeout=0; PRAGMA locking_mode=EXCLUSIVE; BEGIN EXCLUSIVE;"
            )?;
        }
        let receipt = read_apply(&databases[0])?;
        let procs = &databases[1..];
        let digests = procs
            .iter()
            .map(|db| state_digest(&read_state(db)?))
            .collect::<Result<Vec<_>>>()?;
        let winner = (0..3)
            .find(|i| {
                digests
                    .iter()
                    .filter(|digest| **digest == digests[*i])
                    .count()
                    >= 2
            })
            .ok_or_else(|| Error::new(Code::MonsterCorrupt, "DB1 states have no majority"))?;
        let donor_digest = digests[winner];

        // Validate the real certification operation against every majority copy,
        // then undo it so the original majority remains available during adoption.
        let mut applied = None;
        for (db, digest) in procs.iter().zip(&digests) {
            if *digest != donor_digest {
                continue;
            }
            db.execute_batch("SAVEPOINT monsterft_certify")?;
            applied = Some(clear_fence(db, &receipt, allow_unresolved)?);
            db.execute_batch("ROLLBACK TO monsterft_certify; RELEASE monsterft_certify;")?;
        }
        let applied = applied.expect("majority has at least two members");

        // Repair the outlier before certifying any majority member. Each separate
        // commit therefore leaves a recoverable majority if the process stops.
        for (db, digest) in procs.iter().zip(&digests) {
            if *digest == donor_digest {
                continue;
            }
            copy_db1_into(db, &procs[winner], &donor_digest)?;
            db.execute_batch("COMMIT; BEGIN EXCLUSIVE;")?;
        }
        for db in procs {
            if clear_fence(db, &receipt, allow_unresolved)? != applied {
                return Err(Error::new(
                    Code::MonsterCorrupt,
                    "certified DB1 checkpoints differ",
                ));
            }
            db.execute_batch("COMMIT")?;
        }
        databases[0].execute_batch("COMMIT")?;
        Ok(applied)
    })();

    // Close discards unfinished work. Preserve the first operation failure while
    // attempting every close; earlier commits retain the ordered majority above.
    let mut result = result;
    for db in databases.into_iter().rev() {
        if let Err((_, error)) = db.close()
            && result.is_ok()
        {
            result = Err(error.into());
        }
    }
    result
}
