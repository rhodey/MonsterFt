use axum::{
    Json, Router,
    extract::{
        Query, State,
        rejection::{JsonRejection, QueryRejection},
    },
    http::{HeaderMap, StatusCode},
    routing::get,
};
use monsterft::rusqlite::{Connection, OptionalExtension};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};

type Database = Arc<Mutex<Connection>>;
type HttpError = (StatusCode, Json<Value>);
type Reply = Result<Json<Value>, HttpError>;

fn error(status: StatusCode, message: impl ToString) -> HttpError {
    let body = json!({
        "error": message.to_string(),
    });

    (status, Json(body))
}

fn internal(error_value: impl ToString) -> HttpError {
    error(StatusCode::INTERNAL_SERVER_ERROR, error_value)
}

async fn method_not_allowed() -> HttpError {
    error(StatusCode::METHOD_NOT_ALLOWED, "method not allowed")
}

pub fn router(db: Connection) -> monsterft::rusqlite::Result<Router> {
    db.execute_batch(
        "
        PRAGMA synchronous = FULL;
        CREATE TABLE IF NOT EXISTS reports (
            request_id TEXT NOT NULL,
            node_id TEXT NOT NULL,
            hash TEXT NOT NULL,
            fact TEXT NOT NULL,
            PRIMARY KEY (request_id, node_id)
        ) STRICT;
        CREATE TABLE IF NOT EXISTS observations (
            request_id TEXT NOT NULL,
            observation_id TEXT NOT NULL,
            result TEXT NOT NULL,
            PRIMARY KEY (request_id, observation_id)
        ) STRICT;
    ",
    )?;

    // Explicit HEAD rejection keeps it from creating a frozen GET observation.
    let app = Router::new()
        .route(
            "/facts",
            get(get_fact).post(post_fact).head(method_not_allowed),
        )
        .fallback(|| async { error(StatusCode::NOT_FOUND, "not found") })
        .method_not_allowed_fallback(method_not_allowed)
        .with_state(Arc::new(Mutex::new(db)));

    Ok(app)
}

async fn post_fact(
    State(db): State<Database>,
    headers: HeaderMap,
    body: Result<Json<Value>, JsonRejection>,
) -> Reply {
    let Json(body) = body.map_err(|_| error(StatusCode::BAD_REQUEST, "invalid JSON body"))?;

    let mut nodes = headers.get_all("MonsterFt-Node-Id").iter();
    let node = nodes.next().and_then(|value| value.to_str().ok());

    let request_id = body
        .get("requestId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty());
    let fact = body.get("fact");

    // Validate the report identity before reading or writing any stored reports.
    let (Some(node @ ("1" | "2" | "3")), Some(request_id), Some(fact)) = (node, request_id, fact)
    else {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "POST requires a node ID, request ID, and fact",
        ));
    };

    if nodes.next().is_some() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "POST requires exactly one node ID",
        ));
    }

    let serialized = fact.to_string();
    let hash: String = Sha256::digest(serialized.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();

    // This lock covers synchronous SQL only, never an HTTP read or await.
    let db = db.lock().map_err(internal)?;

    let previous: Option<String> = db
        .query_row(
            "SELECT hash FROM reports WHERE request_id = ? AND node_id = ?",
            [request_id, node],
            |row| row.get(0),
        )
        .optional()
        .map_err(internal)?;

    if previous.is_some_and(|previous| previous != hash) {
        return Err(error(
            StatusCode::CONFLICT,
            "a node cannot change its fact report",
        ));
    }

    db.execute(
        "INSERT OR IGNORE INTO reports VALUES (?, ?, ?, ?)",
        [request_id, node, &hash, &serialized],
    )
    .map_err(internal)?;

    // Exclude the reporting node so every member receives the same result.
    let result = json!({
        "status": "reported",
        "requestId": request_id,
        "fact": fact,
    });

    Ok(Json(result))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Observation {
    request_id: String,
    observation_id: String,
}

async fn get_fact(
    State(db): State<Database>,
    query: Result<Query<Observation>, QueryRejection>,
) -> Reply {
    let Query(Observation {
        request_id,
        observation_id,
    }) = query.map_err(|_| {
        error(
            StatusCode::BAD_REQUEST,
            "GET requires a request ID and observation ID",
        )
    })?;

    if request_id.is_empty() || observation_id.is_empty() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "GET requires a request ID and observation ID",
        ));
    }

    let db = db.lock().map_err(internal)?;

    let previous: Option<String> = db
        .query_row(
            "SELECT result FROM observations WHERE request_id = ? AND observation_id = ?",
            [&request_id, &observation_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(internal)?;

    if let Some(previous) = previous {
        let observation = serde_json::from_str(&previous).map_err(internal)?;

        return Ok(Json(observation));
    }

    // The primary key gives each node one report, so duplicates cannot form a quorum.
    let agreed: Option<String> = db
        .query_row(
            "SELECT fact FROM reports WHERE request_id = ? GROUP BY hash HAVING count(*) >= 2",
            [&request_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(internal)?;

    let observation = if let Some(fact) = agreed {
        let fact: Value = serde_json::from_str(&fact).map_err(internal)?;

        json!({
            "status": "confirmed",
            "requestId": request_id,
            "observationId": observation_id,
            "fact": fact,
        })
    } else {
        json!({
            "status": "pending",
            "requestId": request_id,
            "observationId": observation_id,
        })
    };

    // Freeze even a pending observation: a later report must not change this CMD's result.
    db.execute(
        "INSERT INTO observations VALUES (?, ?, ?)",
        [&request_id, &observation_id, &observation.to_string()],
    )
    .map_err(internal)?;

    Ok(Json(observation))
}
