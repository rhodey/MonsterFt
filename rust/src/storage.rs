//! Local database ownership, startup majority evidence, and Apply checkpoints.

use crate::{
    error::{Code, Error, Result},
    log::{
        Election, LogRow, SqliteLog, row_error, stored_row, valid_nonnegative, validate_election,
        validate_row,
    },
    wire::{checksum, encode_payload},
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

pub type Digest = [u8; 16];
pub const REPAIR_NONE: i64 = 0;
pub const REPAIR_NO_QUORUM: i64 = 10019;
pub const REPAIR_OUTSIDE: i64 = 10020;
const TABLES: [&str; 3] = ["raft_log", "raft_election", "monsterft_meta"];

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ProcRole {
    #[serde(rename = "procA")]
    A,
    #[serde(rename = "procB")]
    B,
    #[serde(rename = "procC")]
    C,
}

impl ProcRole {
    pub const ALL: [Self; 3] = [Self::A, Self::B, Self::C];
    pub fn index(self) -> usize {
        match self {
            Self::A => 0,
            Self::B => 1,
            Self::C => 2,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::A => "procA",
            Self::B => "procB",
            Self::C => "procC",
        }
    }
}

#[derive(Clone, Debug)]
pub struct DatabasePaths {
    pub proc_a: PathBuf,
    pub proc_b: PathBuf,
    pub proc_c: PathBuf,
    pub apply: PathBuf,
}

impl DatabasePaths {
    pub fn proc(&self, role: ProcRole) -> &Path {
        match role {
            ProcRole::A => &self.proc_a,
            ProcRole::B => &self.proc_b,
            ProcRole::C => &self.proc_c,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Receipt {
    pub seq: i64,
    #[serde(with = "optional_digest")]
    pub entry_hash: Option<Digest>,
    #[serde(with = "optional_digest")]
    pub local_digest: Option<Digest>,
}

impl Receipt {
    pub fn empty() -> Self {
        Self {
            seq: -1,
            entry_hash: None,
            local_digest: None,
        }
    }
    pub fn validate(&self) -> Result<()> {
        let valid = if self.seq == -1 {
            self.entry_hash.is_none() && self.local_digest.is_none()
        } else {
            self.seq >= 0 && self.entry_hash.is_some() && self.local_digest.is_some()
        };
        if !valid {
            return Err(corrupt("apply receipt is illegal"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Meta {
    pub applied_seq: i64,
    #[serde(with = "optional_digest")]
    pub applied_entry_hash: Option<Digest>,
    pub repair_state: i64,
    pub pending_cmd_seq: Option<i64>,
    #[serde(with = "optional_digest")]
    pub applied_local_digest: Option<Digest>,
}

impl Meta {
    pub fn validate(&self) -> Result<()> {
        let valid = if self.applied_seq == -1 {
            self.applied_entry_hash.is_none()
                && self.applied_local_digest.is_none()
                && self.repair_state == REPAIR_NONE
        } else {
            self.applied_seq >= 0
                && self.applied_entry_hash.is_some()
                && self.applied_local_digest.is_some()
                && [REPAIR_NONE, REPAIR_NO_QUORUM, REPAIR_OUTSIDE].contains(&self.repair_state)
        };
        if !valid {
            return Err(corrupt("DB1 meta is illegal"));
        }
        if self
            .pending_cmd_seq
            .is_some_and(|seq| seq < 0 || seq > self.applied_seq)
        {
            return Err(corrupt("pending command sequence is corrupt"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct State {
    pub election: Election,
    pub meta: Meta,
    pub head: Option<LogRow>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capture {
    #[serde(
        serialize_with = "serde_bytes::serialize",
        deserialize_with = "crate::binary::hash"
    )]
    pub state: Digest,
    #[serde(
        serialize_with = "serde_bytes::serialize",
        deserialize_with = "crate::binary::hash"
    )]
    pub receipt: Digest,
    pub donor: ProcRole,
}

#[derive(Clone)]
struct Captured {
    own_digest: Digest,
    selected_digest: Digest,
    donor: ProcRole,
    receipt: Receipt,
}

pub struct ProtocolStore {
    pub role: ProcRole,
    pub paths: DatabasePaths,
    pub log: SqliteLog,
    generation: Option<Digest>,
    captured: Option<Captured>,
    adopted: bool,
}

fn corrupt(message: impl Into<String>) -> Error {
    Error::new(Code::MonsterCorrupt, message)
}

pub fn validate_generation(generation: &[u8; 16]) -> Result<()> {
    if generation.iter().all(|byte| *byte == 0) {
        return Err(Error::new(
            Code::ArgumentIllegal,
            "generation must be nonzero 16-byte data",
        ));
    }
    Ok(())
}

pub fn database_paths(base: impl AsRef<Path>) -> Result<DatabasePaths> {
    let base = base.as_ref();
    if base.as_os_str().is_empty() || base == Path::new(":memory:") {
        return Err(Error::new(
            Code::ArgumentIllegal,
            "databasePath must be a non-empty file path",
        ));
    }
    let suffix = |s| {
        let mut path = base.as_os_str().to_owned();
        path.push(s);
        PathBuf::from(path)
    };
    Ok(DatabasePaths {
        proc_a: suffix("1a"),
        proc_b: suffix("1b"),
        proc_c: suffix("1c"),
        apply: suffix("2"),
    })
}

pub fn file_exists(path: &Path) -> Result<bool> {
    match std::fs::metadata(path) {
        Ok(stat) if stat.is_file() => Ok(true),
        Ok(_) => Err(corrupt(format!(
            "database is not a file: {}",
            path.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn schema(db: &Connection) -> Result<Vec<(String, String)>> {
    let mut statement = db.prepare("SELECT name,type FROM sqlite_schema")?;
    Ok(statement
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?)
}

pub fn read_database<T>(path: &Path, read: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
    if !file_exists(path)? {
        return Err(corrupt(format!("database is missing: {}", path.display())));
    }
    let db = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    db.busy_timeout(std::time::Duration::ZERO)?;
    db.execute_batch("BEGIN")?;
    let result = read(&db)?;
    db.execute_batch("COMMIT")?;
    db.close().map_err(|(_, error)| Error::from(error))?;
    Ok(result)
}

fn require_table(db: &Connection, name: &str, code: Code) -> Result<()> {
    let kind: Option<String> = db
        .prepare_cached("SELECT type FROM sqlite_schema WHERE name=?")?
        .query_row([name], |r| r.get(0))
        .optional()?;
    if kind.as_deref() != Some("table") {
        return Err(Error::new(code, format!("{name} table is missing")));
    }
    Ok(())
}

fn digest_column(value: Option<Vec<u8>>, name: &str) -> Result<Option<Digest>> {
    value
        .map(|bytes| {
            bytes
                .try_into()
                .map_err(|_| corrupt(format!("{name} is illegal")))
        })
        .transpose()
}

fn stored_generation(bytes: Vec<u8>, name: &str) -> Result<Digest> {
    let generation: Digest = bytes
        .try_into()
        .map_err(|_| corrupt(format!("{name} generation is illegal")))?;
    if generation.iter().all(|byte| *byte == 0) {
        return Err(corrupt(format!("{name} generation is illegal")));
    }
    Ok(generation)
}

pub fn read_apply(db: &Connection) -> Result<Receipt> {
    require_table(db, "monsterft_meta_apply", Code::MonsterCorrupt)?;
    let mut statement = db.prepare_cached(
        "SELECT id,generation,seq,entry_hash,local_digest FROM monsterft_meta_apply",
    )?;
    let mut rows = statement.query([])?;
    let row = rows
        .next()?
        .ok_or_else(|| corrupt("apply meta row is missing or not singleton"))?;
    let fields = (|| -> rusqlite::Result<_> {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Vec<u8>>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, Option<Vec<u8>>>(3)?,
            row.get::<_, Option<Vec<u8>>>(4)?,
        ))
    })()
    .map_err(|e| row_error(e, Code::MonsterCorrupt, "apply receipt"))?;
    if fields.0 != 1 || rows.next()?.is_some() {
        return Err(corrupt("apply meta row is missing or not singleton"));
    }
    stored_generation(fields.1, "DB2")?;
    let receipt = Receipt {
        seq: fields.2,
        entry_hash: digest_column(fields.3, "apply receipt entry hash")?,
        local_digest: digest_column(fields.4, "apply receipt local digest")?,
    };
    receipt.validate()?;
    Ok(receipt)
}

pub fn read_meta(db: &Connection) -> Result<Meta> {
    require_table(db, "monsterft_meta", Code::MonsterCorrupt)?;
    let mut statement = db.prepare("SELECT id,generation,applied_seq,applied_entry_hash,repair_state,pending_cmd_seq,applied_local_digest FROM monsterft_meta")?;
    let mut rows = statement.query([])?;
    let row = rows
        .next()?
        .ok_or_else(|| corrupt("meta row is missing or not singleton"))?;
    let fields = (|| -> rusqlite::Result<_> {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Vec<u8>>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, Option<Vec<u8>>>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, Option<i64>>(5)?,
            row.get::<_, Option<Vec<u8>>>(6)?,
        ))
    })()
    .map_err(|e| row_error(e, Code::MonsterCorrupt, "DB1 meta"))?;
    if fields.0 != 1 || rows.next()?.is_some() {
        return Err(corrupt("meta row is missing or not singleton"));
    }
    stored_generation(fields.1, "DB1")?;
    let meta = Meta {
        applied_seq: fields.2,
        applied_entry_hash: digest_column(fields.3, "applied entry hash")?,
        repair_state: fields.4,
        pending_cmd_seq: fields.5,
        applied_local_digest: digest_column(fields.6, "applied local digest")?,
    };
    meta.validate()?;
    Ok(meta)
}

pub fn read_state(db: &Connection) -> Result<State> {
    for name in TABLES {
        require_table(
            db,
            name,
            if name == "monsterft_meta" {
                Code::MonsterCorrupt
            } else {
                Code::LogCorrupt
            },
        )?;
    }
    let mut statement = db.prepare("SELECT id,current_term,voted_for FROM raft_election")?;
    let mut rows = statement.query([])?;
    let row = rows
        .next()?
        .ok_or_else(|| Error::new(Code::LogCorrupt, "election row is missing or not singleton"))?;
    let (id, election) = (|| -> rusqlite::Result<_> {
        Ok((
            row.get::<_, i64>(0)?,
            Election {
                term: row.get(1)?,
                voted_for: row.get(2)?,
            },
        ))
    })()
    .map_err(|e| row_error(e, Code::LogCorrupt, "stored election"))?;
    if id != 1 || rows.next()?.is_some() {
        return Err(Error::new(
            Code::LogCorrupt,
            "election row is missing or not singleton",
        ));
    }
    validate_election(&election, Code::LogCorrupt)?;
    let head = db
        .query_row(
            "SELECT seq,term,entry FROM raft_log ORDER BY seq DESC LIMIT 1",
            [],
            stored_row,
        )
        .optional()
        .map_err(|e| row_error(e, Code::LogCorrupt, "stored log head"))?;
    if let Some(head) = &head {
        validate_row(head, None)?;
        let first: i64 = db
            .query_row("SELECT seq FROM raft_log ORDER BY seq LIMIT 1", [], |r| {
                r.get(0)
            })
            .map_err(|e| row_error(e, Code::LogCorrupt, "stored log begin"))?;
        valid_nonnegative(first, "stored log begin", Code::LogCorrupt)?;
    }
    Ok(State {
        election,
        meta: read_meta(db)?,
        head,
    })
}

fn binary(hash: Option<Digest>) -> rmpv::Value {
    hash.map_or(rmpv::Value::Nil, |bytes| {
        rmpv::Value::Binary(bytes.to_vec())
    })
}

pub fn receipt_digest(receipt: &Receipt) -> Result<Digest> {
    receipt.validate()?;
    Ok(checksum(&encode_payload(&rmpv::Value::Array(vec![
        receipt.seq.into(),
        binary(receipt.entry_hash),
        binary(receipt.local_digest),
    ]))?))
}

pub fn state_digest(state: &State) -> Result<Digest> {
    let head = state.head.as_ref().map_or(rmpv::Value::Nil, |h| {
        rmpv::Value::Array(vec![
            h.seq.into(),
            h.term.into(),
            rmpv::Value::Binary(h.entry.clone()),
        ])
    });
    let fields = vec![
        state.election.term.into(),
        state
            .election
            .voted_for
            .as_ref()
            .map_or(rmpv::Value::Nil, |id| id.as_str().into()),
        state.meta.applied_seq.into(),
        binary(state.meta.applied_entry_hash),
        state.meta.repair_state.into(),
        state
            .meta
            .pending_cmd_seq
            .map_or(rmpv::Value::Nil, Into::into),
        binary(state.meta.applied_local_digest),
        head,
    ];
    Ok(checksum(&encode_payload(&rmpv::Value::Array(fields))?))
}

pub fn digest_entry(term: i64, entry: &[u8]) -> Digest {
    let mut hash = xxhash_rust::xxh3::Xxh3::new();
    hash.update(&term.to_le_bytes());
    hash.update(entry);
    hash.digest128().to_be_bytes()
}

pub fn check_locals(paths: &DatabasePaths) -> Result<()> {
    let mut incomplete = false;
    let mut pristine = true;
    for role in ProcRole::ALL
        .into_iter()
        .map(Some)
        .chain(std::iter::once(None))
    {
        let path = role.map_or(paths.apply.as_path(), |role| paths.proc(role));
        if !file_exists(path)? {
            incomplete = true;
            continue;
        }
        read_database(path, |db| {
            let tables = schema(db)?;
            if tables.is_empty() {
                incomplete = true;
                return Ok(());
            }
            if role.is_some() {
                let state = read_state(db)?;
                pristine &= state.head.is_none()
                    && state.election.term == 0
                    && state.election.voted_for.is_none()
                    && state.meta.applied_seq == -1
                    && tables
                        .iter()
                        .all(|(name, kind)| kind == "table" && TABLES.contains(&name.as_str()));
            } else {
                let receipt = read_apply(db)?;
                pristine &= receipt.seq == -1
                    && tables
                        .iter()
                        .all(|(name, kind)| kind == "table" && name == "monsterft_meta_apply");
            }
            Ok(())
        })?;
    }
    if incomplete && !pristine {
        return Err(corrupt(
            "missing database or schema beside established state",
        ));
    }
    Ok(())
}

pub fn create_apply(db: &Connection, generation: &Digest) -> Result<()> {
    validate_generation(generation)?;
    db.execute_batch("CREATE TABLE monsterft_meta_apply (
        id INTEGER PRIMARY KEY CHECK(id=1), generation BLOB NOT NULL CHECK(length(generation)=16),
        seq INTEGER NOT NULL, entry_hash BLOB, local_digest BLOB,
        CHECK((seq=-1 AND entry_hash IS NULL AND local_digest IS NULL) OR
            (seq>=0 AND entry_hash IS NOT NULL AND length(entry_hash)=16 AND local_digest IS NOT NULL AND length(local_digest)=16))
    ) STRICT;")?;
    db.execute("INSERT INTO monsterft_meta_apply(id,generation,seq,entry_hash,local_digest) VALUES(1,?,-1,NULL,NULL)", [generation.as_slice()])?;
    Ok(())
}

pub fn create_meta(db: &Connection, generation: &Digest) -> Result<()> {
    validate_generation(generation)?;
    db.execute_batch("CREATE TABLE monsterft_meta (
        id INTEGER PRIMARY KEY CHECK(id=1), generation BLOB NOT NULL CHECK(length(generation)=16),
        applied_seq INTEGER NOT NULL, applied_entry_hash BLOB,
        repair_state INTEGER NOT NULL CHECK(repair_state IN(0,10019,10020)), pending_cmd_seq INTEGER, applied_local_digest BLOB,
        CHECK((applied_seq=-1 AND applied_entry_hash IS NULL AND applied_local_digest IS NULL) OR
            (applied_seq>=0 AND applied_entry_hash IS NOT NULL AND length(applied_entry_hash)=16 AND applied_local_digest IS NOT NULL AND length(applied_local_digest)=16)),
        CHECK(pending_cmd_seq IS NULL OR(pending_cmd_seq>=0 AND pending_cmd_seq<=applied_seq))
    ) STRICT;")?;
    db.execute("INSERT INTO monsterft_meta(id,generation,applied_seq,applied_entry_hash,repair_state,pending_cmd_seq,applied_local_digest) VALUES(1,?,-1,NULL,0,NULL,NULL)", [generation.as_slice()])?;
    Ok(())
}

fn generation_queries(table: &str) -> Result<(&'static str, &'static str, &'static str)> {
    match table {
        "monsterft_meta" => Ok((
            "SELECT generation FROM monsterft_meta WHERE id=1",
            "UPDATE monsterft_meta SET generation=? WHERE id=1",
            "DB1",
        )),
        "monsterft_meta_apply" => Ok((
            "SELECT generation FROM monsterft_meta_apply WHERE id=1",
            "UPDATE monsterft_meta_apply SET generation=? WHERE id=1",
            "DB2",
        )),
        _ => Err(Error::new(
            Code::ArgumentIllegal,
            "generation table is illegal",
        )),
    }
}

pub fn check_generation(db: &Connection, table: &str, generation: &Digest) -> Result<()> {
    let (query, _, name) = generation_queries(table)?;
    // This SELECT is on every ordered input's hot path; reuse its prepared statement.
    let bytes: Vec<u8> = db
        .prepare_cached(query)?
        .query_row([], |r| r.get(0))
        .optional()
        .map_err(|e| row_error(e, Code::MonsterCorrupt, "stored generation"))?
        .ok_or_else(|| corrupt(format!("{name} generation is missing")))?;
    if stored_generation(bytes, name)? != *generation {
        return Err(Error::new(
            Code::GenerationDiff,
            format!("{name} generation different"),
        ));
    }
    Ok(())
}

pub fn claim_generation(db: &Connection, table: &str, generation: &Digest) -> Result<()> {
    validate_generation(generation)?;
    if db.is_autocommit() {
        return Err(Error::new(
            Code::MonsterIllegal,
            "generation claim requires a transaction",
        ));
    }
    let (_, query, _) = generation_queries(table)?;
    if db.prepare_cached(query)?.execute([generation.as_slice()])? != 1 {
        return Err(corrupt(format!("{table} row is missing")));
    }
    Ok(())
}

pub fn open_apply_database(base: &Path, generation: &Digest) -> Result<Connection> {
    validate_generation(generation)?;
    let paths = database_paths(base)?;
    check_locals(&paths)?;
    if let Some(parent) = paths.apply.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    let db = Connection::open(paths.apply)?;
    db.busy_timeout(std::time::Duration::ZERO)?;
    db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; BEGIN IMMEDIATE;")?;
    if schema(&db)?.is_empty() {
        create_apply(&db, generation)?;
    }
    read_apply(&db)?;
    claim_generation(&db, "monsterft_meta_apply", generation)?;
    db.execute_batch("COMMIT")?;
    Ok(db)
}

pub fn write_receipt(db: &Connection, receipt: &Receipt) -> Result<()> {
    receipt.validate()?;
    if db.is_autocommit() {
        return Err(Error::new(
            Code::MonsterIllegal,
            "Apply receipt requires a transaction",
        ));
    }
    let changed = db.prepare_cached("UPDATE monsterft_meta_apply SET seq=?,entry_hash=?,local_digest=? WHERE id=1 AND seq<?")?
        .execute(params![receipt.seq, receipt.entry_hash.as_ref().map(|h| h.as_slice()), receipt.local_digest.as_ref().map(|h| h.as_slice()), receipt.seq])?;
    if changed != 1 {
        return Err(Error::new(
            Code::SqliteError,
            "Apply receipt update count not one",
        ));
    }
    Ok(())
}

pub fn verify_entry(db: &Connection, seq: i64, hash: &Digest, name: &str) -> Result<()> {
    let row = db
        .prepare_cached("SELECT seq,term,entry FROM raft_log WHERE seq=?")?
        .query_row([seq], stored_row)
        .optional()
        .map_err(|e| row_error(e, Code::LogCorrupt, "stored log"))?
        .ok_or_else(|| corrupt(format!("{name} log entry not found")))?;
    validate_row(&row, Some(seq))?;
    if digest_entry(row.term, &row.entry) != *hash {
        return Err(corrupt(format!("{name} entry hash does not match")));
    }
    Ok(())
}

pub fn copy_apply_into(db: &Connection, receipt: &Receipt) -> Result<()> {
    receipt.validate()?;
    let previous = receipt
        .seq
        .checked_sub(1)
        .ok_or_else(|| corrupt("apply receipt sequence is illegal"))?;
    let changed = db.prepare_cached("UPDATE monsterft_meta SET applied_seq=?,applied_entry_hash=?,pending_cmd_seq=?,applied_local_digest=?
        WHERE id=1 AND applied_seq=? AND pending_cmd_seq IS NULL AND repair_state=0")?
        .execute(params![receipt.seq, receipt.entry_hash.as_ref().map(|h| h.as_slice()), if receipt.seq == 0 {None} else {Some(receipt.seq)},
            receipt.local_digest.as_ref().map(|h| h.as_slice()), previous])?;
    if changed != 1 {
        return Err(Error::new(
            Code::SqliteError,
            "DB1 copy apply update count not one",
        ));
    }
    Ok(())
}

pub fn reconcile_receipt(db: &Connection, receipt: &Receipt) -> Result<Meta> {
    receipt.validate()?;
    let meta = read_meta(db)?;
    if meta.applied_seq >= 0 {
        verify_entry(
            db,
            meta.applied_seq,
            &meta.applied_entry_hash.expect("validated hash"),
            "applied",
        )?;
    }
    if receipt.seq > meta.applied_seq {
        if Some(receipt.seq) != meta.applied_seq.checked_add(1) {
            return Err(corrupt(
                "apply receipt is more than one entry ahead of DB1 checkpoint",
            ));
        }
        if meta.pending_cmd_seq.is_some() {
            return Err(corrupt(
                "apply receipt is ahead of DB1 checkpoint while a CMD is pending",
            ));
        }
        if meta.repair_state != REPAIR_NONE {
            return Err(corrupt(
                "apply receipt is ahead of DB1 checkpoint while a repair fence is set",
            ));
        }
        verify_entry(
            db,
            receipt.seq,
            &receipt.entry_hash.expect("validated hash"),
            "apply",
        )?;
        copy_apply_into(db, receipt)?;
        return read_meta(db);
    }
    if receipt.seq == -1 {
        if meta.applied_seq != -1 {
            return Err(corrupt(
                "apply receipt seq is -1 but DB1 applied_seq is not",
            ));
        }
        return Ok(meta);
    }
    if meta.applied_local_digest != receipt.local_digest {
        return Err(corrupt("apply local digest does not match"));
    }
    if meta.pending_cmd_seq.is_some_and(|seq| seq != receipt.seq) {
        return Err(corrupt("pending command seq does not match apply receipt"));
    }
    let first: i64 = db
        .query_row("SELECT seq FROM raft_log ORDER BY seq LIMIT 1", [], |r| {
            r.get(0)
        })
        .optional()
        .map_err(|e| row_error(e, Code::LogCorrupt, "stored log begin"))?
        .ok_or_else(|| Error::new(Code::LogCorrupt, "stored log begin is missing"))?;
    valid_nonnegative(first, "stored log begin", Code::LogCorrupt)?;
    if receipt.seq >= first {
        verify_entry(
            db,
            receipt.seq,
            &receipt.entry_hash.expect("validated hash"),
            "apply",
        )?;
    }
    Ok(meta)
}

pub fn copy_db1_into(db: &Connection, donor: &Connection, donor_digest: &Digest) -> Result<()> {
    if db.is_autocommit() {
        return Err(Error::new(
            Code::MonsterIllegal,
            "DB1 copy requires a transaction",
        ));
    }
    let state = read_state(donor)?;
    if state_digest(&state)? != *donor_digest {
        return Err(corrupt("DB1 donor changed after capture"));
    }
    db.execute_batch("DELETE FROM raft_log; DELETE FROM raft_election;")?;
    let mut query = donor.prepare("SELECT seq,term,entry FROM raft_log ORDER BY seq")?;
    let mut insert = db.prepare("INSERT INTO raft_log(seq,term,entry) VALUES(?,?,?)")?;
    let mut previous: Option<i64> = None;
    for row in query.query_map([], stored_row)? {
        let row = row.map_err(|e| row_error(e, Code::LogCorrupt, "stored log"))?;
        validate_row(&row, None)?;
        if previous.is_some_and(|seq| seq.checked_add(1) != Some(row.seq)) {
            return Err(Error::new(Code::LogCorrupt, "stored log row is corrupt"));
        }
        insert.execute(params![row.seq, row.term, row.entry])?;
        previous = Some(row.seq);
    }
    db.execute(
        "INSERT INTO raft_election(id,current_term,voted_for) VALUES(1,?,?)",
        params![state.election.term, state.election.voted_for],
    )?;
    // Generation belongs to the destination owner, not the donor's protocol state.
    let meta = state.meta;
    let changed = db.execute("UPDATE monsterft_meta SET applied_seq=?,applied_entry_hash=?,repair_state=?,pending_cmd_seq=?,applied_local_digest=? WHERE id=1",
        params![meta.applied_seq,meta.applied_entry_hash.as_ref().map(|h|h.as_slice()),meta.repair_state,meta.pending_cmd_seq,meta.applied_local_digest.as_ref().map(|h|h.as_slice())])?;
    if changed != 1 {
        return Err(corrupt("meta row is missing"));
    }
    if state_digest(&read_state(db)?)? != *donor_digest {
        return Err(corrupt("adopted DB1 state does not match majority"));
    }
    Ok(())
}

impl ProtocolStore {
    pub fn new(role: ProcRole, base: impl AsRef<Path>, iter_step: usize) -> Result<Self> {
        let paths = database_paths(base)?;
        let log = SqliteLog::new(paths.proc(role), iter_step)?;
        Ok(Self {
            role,
            paths,
            log,
            generation: None,
            captured: None,
            adopted: false,
        })
    }

    pub fn db(&self) -> Result<&Connection> {
        self.log.db()
    }
    pub fn load(&self) -> Result<Meta> {
        read_meta(self.db()?)
    }
    pub fn check_generation(&self) -> Result<()> {
        let generation = self
            .generation
            .as_ref()
            .ok_or_else(|| Error::new(Code::MonsterIllegal, "generation has not been claimed"))?;
        check_generation(self.db()?, "monsterft_meta", generation)
    }

    pub fn open(&mut self, generation: &Digest) -> Result<()> {
        validate_generation(generation)?;
        if self.log.is_open() {
            return self.check_generation();
        }
        check_locals(&self.paths)?;
        self.log.open(|db| create_meta(db, generation))?;
        self.db()?.execute_batch("BEGIN IMMEDIATE")?;
        read_meta(self.db()?)?;
        claim_generation(self.db()?, "monsterft_meta", generation)?;
        self.log.read_head()?;
        // Prepare once here; subsequent checks reuse the connection's statement cache.
        self.db()?
            .prepare_cached("SELECT generation FROM monsterft_meta WHERE id=1")?;
        self.db()?.execute_batch("COMMIT")?;
        self.generation = Some(*generation);
        Ok(())
    }

    pub fn capture(&mut self) -> Result<Capture> {
        self.db()?;
        let generation = self
            .generation
            .ok_or_else(|| Error::new(Code::MonsterIllegal, "generation has not been claimed"))?;
        let mut digests = Vec::with_capacity(3);
        for role in ProcRole::ALL {
            digests.push(read_database(self.paths.proc(role), |db| {
                check_generation(db, "monsterft_meta", &generation)?;
                state_digest(&read_state(db)?)
            })?);
        }
        let winner = (0..3)
            .find(|i| {
                digests
                    .iter()
                    .filter(|digest| **digest == digests[*i])
                    .count()
                    >= 2
            })
            .ok_or_else(|| corrupt("DB1 states have no majority"))?;
        let receipt = read_database(&self.paths.apply, |db| {
            check_generation(db, "monsterft_meta_apply", &generation)?;
            read_apply(db)
        })?;
        let selected_digest = digests[winner];
        let donor = ProcRole::ALL[winner];
        let result = Capture {
            state: selected_digest,
            receipt: receipt_digest(&receipt)?,
            donor,
        };
        self.captured = Some(Captured {
            own_digest: digests[self.role.index()],
            selected_digest,
            donor,
            receipt,
        });
        self.adopted = false;
        Ok(result)
    }

    pub fn adopt(&mut self) -> Result<()> {
        let captured = self
            .captured
            .clone()
            .ok_or_else(|| Error::new(Code::MonsterIllegal, "DB1 state has not been captured"))?;
        self.db()?.execute_batch("BEGIN IMMEDIATE")?;
        self.check_generation()?;
        if state_digest(&read_state(self.db()?)?)? != captured.own_digest {
            return Err(corrupt("DB1 changed after capture"));
        }
        if captured.own_digest != captured.selected_digest {
            if captured.donor == self.role {
                return Err(Error::new(Code::MonsterIllegal, "DB1 donor is illegal"));
            }
            let other = ProcRole::ALL
                .into_iter()
                .find(|role| *role != self.role && *role != captured.donor)
                .expect("three distinct Proc roles");
            let generation = self.generation.expect("open generation");
            read_database(self.paths.proc(captured.donor), |donor| {
                check_generation(donor, "monsterft_meta", &generation)?;
                read_database(self.paths.proc(other), |witness| {
                    check_generation(witness, "monsterft_meta", &generation)?;
                    if state_digest(&read_state(witness)?)? != captured.selected_digest {
                        return Err(corrupt(format!(
                            "DB1 {} does not match selected state",
                            other.as_str()
                        )));
                    }
                    copy_db1_into(self.db()?, donor, &captured.selected_digest)
                })
            })?;
        }
        if state_digest(&read_state(self.db()?)?)? != captured.selected_digest {
            return Err(corrupt("adopted DB1 state does not match majority"));
        }
        self.log.read_head()?;
        self.db()?.execute_batch("COMMIT")?;
        self.adopted = true;
        Ok(())
    }

    pub fn reconcile(&mut self) -> Result<Meta> {
        if !self.adopted {
            return Err(Error::new(
                Code::MonsterIllegal,
                "DB1 state has not been adopted",
            ));
        }
        let receipt = &self.captured.as_ref().expect("adopted capture").receipt;
        self.db()?.execute_batch("BEGIN IMMEDIATE")?;
        self.check_generation()?;
        let meta = reconcile_receipt(self.db()?, receipt)?;
        self.db()?.execute_batch("COMMIT")?;
        Ok(meta)
    }

    pub fn close(&mut self) -> Result<()> {
        self.captured = None;
        self.adopted = false;
        self.generation = None;
        self.log.close()
    }
}

pub mod optional_digest {
    use serde::{Deserializer, Serializer};
    pub fn serialize<S: Serializer>(
        value: &Option<[u8; 16]>,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        match value {
            Some(bytes) => serializer.serialize_some(serde_bytes::Bytes::new(bytes)),
            None => serializer.serialize_none(),
        }
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Option<[u8; 16]>, D::Error> {
        crate::binary::optional_hash(deserializer)
    }
}
