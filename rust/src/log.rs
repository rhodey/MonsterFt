//! The durable Raft log. Mutation belongs to its Proc's ordered-input transaction.

use crate::error::{Code, Error, Result};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: i64,
    #[serde(
        serialize_with = "serde_bytes::serialize",
        deserialize_with = "crate::binary::vec"
    )]
    pub entry: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogRow {
    pub seq: i64,
    pub term: i64,
    #[serde(
        serialize_with = "serde_bytes::serialize",
        deserialize_with = "crate::binary::vec"
    )]
    pub entry: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Election {
    pub term: i64,
    pub voted_for: Option<String>,
}

pub struct SqliteLog {
    pub path: PathBuf,
    pub seq: i64,
    pub term: i64,
    pub head: Option<Vec<u8>>,
    pub begin: i64,
    pub election: Election,
    iter_step: usize,
    connection: Option<Connection>,
}

pub(crate) fn valid_nonnegative(value: i64, name: &str, code: Code) -> Result<()> {
    if value < 0 {
        return Err(Error::new(code, format!("{name} must be >= 0")));
    }
    Ok(())
}

pub(crate) fn validate_election(election: &Election, code: Code) -> Result<()> {
    valid_nonnegative(election.term, "term", code)?;
    if election.voted_for.as_ref().is_some_and(String::is_empty) {
        return Err(Error::new(
            code,
            "votedFor must be null or a non-empty string",
        ));
    }
    Ok(())
}

pub(crate) fn stored_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<LogRow> {
    Ok(LogRow {
        seq: row.get(0)?,
        term: row.get(1)?,
        entry: row.get(2)?,
    })
}

pub(crate) fn validate_row(row: &LogRow, expected: Option<i64>) -> Result<()> {
    valid_nonnegative(row.seq, "stored log seq", Code::LogCorrupt)?;
    valid_nonnegative(row.term, "stored log term", Code::LogCorrupt)?;
    if let Some(expected) = expected
        && row.seq != expected
    {
        return Err(Error::new(
            Code::LogCorrupt,
            format!("seq {} !== {expected}", row.seq),
        ));
    }
    Ok(())
}

pub(crate) fn row_error(error: rusqlite::Error, code: Code, context: &str) -> Error {
    match error {
        rusqlite::Error::InvalidColumnType(..)
        | rusqlite::Error::IntegralValueOutOfRange(..)
        | rusqlite::Error::FromSqlConversionFailure(..) => {
            Error::new(code, format!("{context}: {error}"))
        }
        other => other.into(),
    }
}

impl SqliteLog {
    pub fn new(path: impl AsRef<Path>, iter_step: usize) -> Result<Self> {
        let path = path.as_ref();
        if path.as_os_str().is_empty() || path == Path::new(":memory:") {
            return Err(Error::new(
                Code::ArgumentIllegal,
                "SQLiteLog DB path must be a non-empty file path",
            ));
        }
        if iter_step == 0 || iter_step > i64::MAX as usize {
            return Err(Error::new(
                Code::ArgumentIllegal,
                "iterStepSize must be int > 0",
            ));
        }
        Ok(Self {
            path: path.to_owned(),
            seq: -1,
            term: -1,
            head: None,
            begin: -1,
            election: Election {
                term: 0,
                voted_for: None,
            },
            iter_step,
            connection: None,
        })
    }

    pub fn is_open(&self) -> bool {
        self.connection.is_some()
    }

    pub fn db(&self) -> Result<&Connection> {
        self.connection
            .as_ref()
            .ok_or_else(|| Error::new(Code::LogNotOpen, "log not open"))
    }

    pub fn open(&mut self, initialize: impl FnOnce(&Connection) -> Result<()>) -> Result<()> {
        if self.is_open() {
            return Ok(());
        }
        if let Some(parent) = self.path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let db = Connection::open(&self.path)?;
        db.busy_timeout(std::time::Duration::ZERO)?;
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
        let schema = crate::storage::schema(&db)?;
        if schema.is_empty() {
            db.execute_batch("BEGIN IMMEDIATE;
                CREATE TABLE raft_log (seq INTEGER PRIMARY KEY, term INTEGER NOT NULL, entry BLOB NOT NULL) STRICT;
                CREATE TABLE raft_election (
                    id INTEGER PRIMARY KEY CHECK(id=1), current_term INTEGER NOT NULL,
                    voted_for TEXT CHECK(voted_for IS NULL OR voted_for <> '')
                ) STRICT;
                INSERT INTO raft_election(id,current_term,voted_for) VALUES(1,0,NULL);")?;
            initialize(&db)?;
            db.execute_batch("COMMIT")?;
        } else {
            for name in ["raft_log", "raft_election"] {
                if !schema.iter().any(|(n, t)| n == name && t == "table") {
                    return Err(Error::new(
                        Code::LogCorrupt,
                        format!("{name} table is missing"),
                    ));
                }
            }
        }
        self.connection = Some(db);
        if let Err(error) = self.read_head() {
            self.connection.take();
            self.clear_head();
            return Err(error);
        }
        Ok(())
    }

    pub fn read_head(&mut self) -> Result<()> {
        let db = self.db()?;
        let head = db
            .prepare_cached("SELECT seq,term,entry FROM raft_log ORDER BY seq DESC LIMIT 1")?
            .query_row([], stored_row)
            .optional()
            .map_err(|e| row_error(e, Code::LogCorrupt, "stored log head"))?;
        if let Some(row) = &head {
            validate_row(row, None)?;
        }
        let election = db
            .prepare_cached("SELECT current_term,voted_for FROM raft_election WHERE id=1")?
            .query_row([], |r| {
                Ok(Election {
                    term: r.get(0)?,
                    voted_for: r.get(1)?,
                })
            })
            .optional()
            .map_err(|e| row_error(e, Code::LogCorrupt, "stored election"))?
            .ok_or_else(|| Error::new(Code::LogCorrupt, "election row is missing"))?;
        validate_election(&election, Code::LogCorrupt)?;
        let begin: Option<i64> = db
            .prepare_cached("SELECT seq FROM raft_log ORDER BY seq LIMIT 1")?
            .query_row([], |r| r.get(0))
            .optional()
            .map_err(|e| row_error(e, Code::LogCorrupt, "stored log begin"))?;
        if let Some(begin) = begin {
            valid_nonnegative(begin, "stored log begin", Code::LogCorrupt)?;
        }
        self.seq = head.as_ref().map_or(-1, |row| row.seq);
        self.term = head.as_ref().map_or(-1, |row| row.term);
        self.head = head.map(|row| row.entry);
        self.begin = begin.unwrap_or(-1);
        self.election = election;
        Ok(())
    }

    pub fn sizes(&self, from: i64, limit: usize) -> Result<Vec<usize>> {
        valid_nonnegative(from, "seq", Code::ArgumentIllegal)?;
        if limit == 0 || limit > i64::MAX as usize {
            return Err(Error::new(Code::ArgumentIllegal, "limit must be int > 0"));
        }
        let mut statement = self.db()?.prepare_cached(
            "SELECT length(entry) FROM raft_log WHERE seq>=? AND seq<=? ORDER BY seq LIMIT ?",
        )?;
        let rows = statement.query_map(params![from, self.seq, limit as i64], |r| {
            r.get::<_, i64>(0)
        })?;
        rows.map(|row| {
            let size = row?;
            usize::try_from(size)
                .map_err(|_| Error::new(Code::LogCorrupt, "stored log size is illegal"))
        })
        .collect()
    }

    /// A bounded range, retaining the JS iterator's gap checks and trailing-range behavior.
    pub fn range(&self, from: i64, through: i64, limit: usize) -> Result<Vec<LogRow>> {
        valid_nonnegative(from, "seq", Code::ArgumentIllegal)?;
        if limit == 0 || limit > i64::MAX as usize {
            return Err(Error::new(Code::ArgumentIllegal, "limit must be int > 0"));
        }
        let mut statement = self.db()?.prepare_cached(
            "SELECT seq,term,entry FROM raft_log WHERE seq>=? AND seq<=? ORDER BY seq LIMIT ?",
        )?;
        let rows = statement.query_map(params![from, through, limit as i64], stored_row)?;
        let mut result = Vec::new();
        for (offset, row) in rows.enumerate() {
            let row = row.map_err(|e| row_error(e, Code::LogCorrupt, "stored log"))?;
            let expected = from
                .checked_add(offset as i64)
                .ok_or_else(|| Error::new(Code::LogCorrupt, "stored log sequence overflow"))?;
            validate_row(&row, Some(expected))?;
            result.push(row);
        }
        Ok(result)
    }

    pub fn iter(&self, from: i64) -> Result<LogIter<'_>> {
        self.db()?;
        valid_nonnegative(from, "seq", Code::ArgumentIllegal)?;
        Ok(LogIter {
            log: self,
            next: Some(from),
            last: self.seq,
            buffered: VecDeque::new(),
        })
    }

    pub fn election(&mut self, term: i64, voted_for: Option<String>) -> Result<()> {
        let election = Election { term, voted_for };
        validate_election(&election, Code::ArgumentIllegal)?;
        self.db()?.prepare_cached("INSERT INTO raft_election(id,current_term,voted_for) VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET current_term=excluded.current_term,voted_for=excluded.voted_for")?
            .execute(params![election.term, election.voted_for])?;
        self.election = election;
        Ok(())
    }

    pub fn trim(&mut self, seq: i64) -> Result<()> {
        if seq < -1 {
            return Err(Error::new(Code::ArgumentIllegal, "seq must be >= -1"));
        }
        self.db()?;
        if self.seq <= seq {
            return Ok(());
        }
        self.db()?
            .prepare_cached("DELETE FROM raft_log WHERE seq > ?")?
            .execute([seq])?;
        self.read_head()
    }

    pub fn append(&mut self, data: &LogEntry, seq: Option<i64>) -> Result<i64> {
        self.db()?;
        valid_nonnegative(data.term, "term", Code::ArgumentIllegal)?;
        let next = self
            .seq
            .checked_add(1)
            .ok_or_else(|| Error::new(Code::ArgumentIllegal, "log sequence overflow"))?;
        let seq = seq.unwrap_or(next);
        if next != seq {
            return Err(Error::new(
                Code::ArgumentIllegal,
                format!("next {next} !== {seq}"),
            ));
        }
        self.db()?
            .prepare_cached("INSERT INTO raft_log(seq,term,entry) VALUES(?,?,?)")?
            .execute(params![seq, data.term, data.entry])?;
        self.seq = seq;
        self.term = data.term;
        self.head = Some(data.entry.clone());
        if self.begin < 0 {
            self.begin = seq;
        }
        Ok(seq)
    }

    pub fn append_batch(&mut self, data: &[LogEntry], seq: Option<i64>) -> Result<i64> {
        let db = self.db()?;
        if data.is_empty() {
            return Err(Error::new(
                Code::ArgumentIllegal,
                "data must be array with length > 0",
            ));
        }
        for entry in data {
            valid_nonnegative(entry.term, "term", Code::ArgumentIllegal)?;
        }
        let next = self
            .seq
            .checked_add(1)
            .ok_or_else(|| Error::new(Code::ArgumentIllegal, "log sequence overflow"))?;
        let seq = seq.unwrap_or(next);
        if seq != next {
            return Err(Error::new(
                Code::ArgumentIllegal,
                format!("next {next} !== {seq}"),
            ));
        }
        let count = i64::try_from(data.len() - 1)
            .map_err(|_| Error::new(Code::ArgumentIllegal, "log batch too large"))?;
        let end = seq
            .checked_add(count)
            .ok_or_else(|| Error::new(Code::ArgumentIllegal, "log sequence overflow"))?;
        if db.is_autocommit() {
            return Err(Error::new(
                Code::RaftIllegal,
                "appendBatch requires a transaction",
            ));
        }
        {
            let mut statement =
                db.prepare_cached("INSERT INTO raft_log(seq,term,entry) VALUES(?,?,?)")?;
            for (offset, entry) in data.iter().enumerate() {
                statement.execute(params![seq + offset as i64, entry.term, entry.entry])?;
            }
        }
        let last = data.last().expect("nonempty checked above");
        self.seq = end;
        self.term = last.term;
        self.head = Some(last.entry.clone());
        if self.begin < 0 {
            self.begin = seq;
        }
        Ok(seq)
    }

    fn clear_head(&mut self) {
        self.seq = -1;
        self.term = -1;
        self.head = None;
        self.begin = -1;
        self.election = Election {
            term: 0,
            voted_for: None,
        };
    }

    pub fn close(&mut self) -> Result<()> {
        let Some(connection) = self.connection.take() else {
            return Ok(());
        };
        self.clear_head();
        connection.close().map_err(|(_, error)| error.into())
    }

    pub fn delete(&self) -> Result<()> {
        if self.is_open() {
            return Err(Error::new(Code::LogOpen, "log is open"));
        }
        for suffix in ["", "-wal", "-shm"] {
            let mut path = self.path.as_os_str().to_owned();
            path.push(suffix);
            match std::fs::remove_file(PathBuf::from(path)) {
                Ok(()) => (),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
}

pub struct LogIter<'a> {
    log: &'a SqliteLog,
    next: Option<i64>,
    last: i64,
    buffered: VecDeque<LogRow>,
}

impl Iterator for LogIter<'_> {
    type Item = Result<LogRow>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.buffered.is_empty() {
            let next = self.next?;
            if next > self.last {
                self.next = None;
                return None;
            }
            match self.log.range(next, self.last, self.log.iter_step) {
                Ok(rows) if rows.is_empty() => {
                    self.next = None;
                    return None;
                }
                Ok(rows) => self.buffered = rows.into(),
                Err(error) => {
                    self.next = None;
                    return Some(Err(error));
                }
            }
        }
        let row = self.buffered.pop_front()?;
        self.next = row.seq.checked_add(1);
        Some(Ok(row))
    }
}
