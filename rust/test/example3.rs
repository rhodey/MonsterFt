#[path = "../example3/facts.rs"]
mod facts;
#[path = "../example3/http.rs"]
mod http;

use monsterft::{Value as Outcome, rusqlite::Connection};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

struct Server {
    url: String,
    client: Client,
    stop: Option<oneshot::Sender<()>>,
    worker: Option<JoinHandle<std::io::Result<()>>>,
}
impl Server {
    async fn start(db: Connection) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/facts", listener.local_addr().unwrap());
        let app = facts::router(db).unwrap();
        let (stop, done) = oneshot::channel();
        let worker = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = done.await;
                })
                .await
        });
        Self {
            url,
            client: Client::builder().no_proxy().build().unwrap(),
            stop: Some(stop),
            worker: Some(worker),
        }
    }
    async fn post(&self, node: &str, request_id: &str, fact: Value) -> reqwest::Response {
        self.client
            .post(&self.url)
            .header("MonsterFt-Node-Id", node)
            .json(&json!({"requestId": request_id, "fact": fact}))
            .send()
            .await
            .unwrap()
    }
    async fn get(&self, request_id: &str, observation_id: &str) -> Value {
        self.client
            .get(&self.url)
            .query(&[("requestId", request_id), ("observationId", observation_id)])
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap()
    }
    async fn close(mut self) {
        self.stop.take().unwrap().send(()).unwrap();
        self.worker.take().unwrap().await.unwrap().unwrap();
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        if let Some(worker) = self.worker.take() {
            worker.abort();
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn reports_are_idempotent_distinct_votes_and_conflicting_reports_are_rejected() {
    let server = Server::start(Connection::open_in_memory().unwrap()).await;
    let fact = json!({"account": "alice", "balance": 42});
    let first: Value = server
        .post("1", "r", fact.clone())
        .await
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let duplicate: Value = server
        .post("1", "r", fact.clone())
        .await
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(first, duplicate);
    assert!(first.get("nodeId").is_none());
    assert_eq!(server.get("r", "before").await["status"], "pending");
    assert_eq!(
        server.post("1", "r", json!({"balance": 0})).await.status(),
        StatusCode::CONFLICT
    );
    server
        .post("2", "r", json!({"balance": 0}))
        .await
        .error_for_status()
        .unwrap();
    assert_eq!(server.get("r", "dissent").await["status"], "pending");
    let third: Value = server
        .post("3", "r", fact.clone())
        .await
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(first, third);
    let confirmed = server.get("r", "after").await;
    assert_eq!(confirmed["status"], "confirmed");
    assert_eq!(confirmed["fact"], fact);
    server.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn pending_and_confirmed_observations_survive_late_reports_and_server_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("facts.db");
    let server = Server::start(Connection::open(&path).unwrap()).await;
    let pending = server.get("r", "early").await;
    assert_eq!(pending["status"], "pending");
    assert!(pending.get("fact").is_none());
    for node in ["1", "2"] {
        server
            .post(node, "r", json!({"account": "alice", "balance": 42}))
            .await
            .error_for_status()
            .unwrap();
    }
    assert_eq!(server.get("r", "early").await, pending);
    let confirmed = server.get("r", "later").await;
    assert_eq!(confirmed["status"], "confirmed");
    server.close().await;
    let db = Connection::open(&path).unwrap();
    let hash: String = db
        .query_row("SELECT hash FROM reports LIMIT 1", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        hash,
        "93e1d663f02267e1448359f07abd3514cba3d70d9d5e9cc233825fd917f5fcb4"
    );
    let server = Server::start(db).await;
    assert_eq!(server.get("r", "early").await, pending);
    assert_eq!(server.get("r", "later").await, confirmed);
    assert_eq!(
        server.post("1", "r", json!(99)).await.status(),
        StatusCode::CONFLICT
    );
    server.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn http_validation_rejects_bad_members_ids_and_requests() {
    let server = Server::start(Connection::open_in_memory().unwrap()).await;
    assert_eq!(
        server.post("4", "r", json!(1)).await.status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        server.post("1", "", json!(1)).await.status(),
        StatusCode::BAD_REQUEST
    );
    let bad = server
        .client
        .post(&server.url)
        .header("MonsterFt-Node-Id", "1")
        .header("MonsterFt-Node-Id", "2")
        .json(&json!({"requestId":"r", "fact":1}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
    for body in [json!({"requestId":"r"}), json!({"requestId":1, "fact":1})] {
        let bad = server
            .client
            .post(&server.url)
            .header("MonsterFt-Node-Id", "1")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
    }
    let bad = server
        .client
        .post(&server.url)
        .header("content-type", "application/json")
        .body("{")
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        server
            .client
            .get(&server.url)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        server
            .client
            .put(&server.url)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::METHOD_NOT_ALLOWED
    );
    assert_eq!(
        server
            .client
            .get(server.url.replace("/facts", "/unknown"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        server
            .client
            .head(&server.url)
            .query(&[("requestId", "null"), ("observationId", "o")])
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::METHOD_NOT_ALLOWED,
    );
    // JSON null is a legitimate fact, distinct from an absent field.
    server
        .post("1", "null", Value::Null)
        .await
        .error_for_status()
        .unwrap();
    server
        .post("2", "null", Value::Null)
        .await
        .error_for_status()
        .unwrap();
    assert_eq!(
        server.get("null", "o").await,
        json!({"status":"confirmed", "requestId":"null", "observationId":"o", "fact":null})
    );
    server.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn async_apply_http_path_preserves_shared_results_and_service_errors() {
    let server = Server::start(Connection::open_in_memory().unwrap()).await;
    assert_eq!(
        http::execute(&server.client, &server.url, "1", None)
            .await
            .unwrap(),
        Outcome::Nil
    );
    let post = json!({"method":"POST", "requestId":"r & +?", "fact":{"balance":42}}).to_string();
    let a = http::execute(&server.client, &server.url, "1", Some(post.as_bytes()))
        .await
        .unwrap();
    let b = http::execute(&server.client, &server.url, "2", Some(post.as_bytes()))
        .await
        .unwrap();
    assert_eq!(a, b);
    let get = json!({"method":"GET", "requestId":"r & +?", "observationId":"o & +?"}).to_string();
    let a = http::execute(&server.client, &server.url, "1", Some(get.as_bytes()))
        .await
        .unwrap();
    let b = http::execute(&server.client, &server.url, "3", Some(get.as_bytes()))
        .await
        .unwrap();
    assert_eq!(a, b);
    let result: Value = rmpv::ext::from_value(a).unwrap();
    assert_eq!(result["status"], "confirmed");
    let conflict = json!({"method":"POST", "requestId":"r & +?", "fact":0}).to_string();
    let error = http::execute(&server.client, &server.url, "1", Some(conflict.as_bytes()))
        .await
        .unwrap_err();
    assert_eq!(error.message, "a node cannot change its fact report");
    assert!(
        http::execute(
            &server.client,
            &server.url,
            "1",
            Some(br#"{"method":"DELETE"}"#)
        )
        .await
        .is_err()
    );
    server.close().await;
}
