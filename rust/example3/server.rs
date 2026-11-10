mod common;
mod facts;

use monsterft::rusqlite::Connection;
use tokio::{
    net::TcpListener,
    signal::unix::{SignalKind, signal},
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = common::data_directory();
    std::fs::create_dir_all(&directory)?;

    // Keep reports and frozen observations across restarts, just like the cluster databases.
    let db = Connection::open(directory.join("facts.db"))?;
    let app = facts::router(db)?;

    let port: u16 = std::env::var("MONSTERFT_HTTP_PORT")
        .unwrap_or_else(|_| "43000".into())
        .parse()?;
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    let mut terminate = signal(SignalKind::terminate())?;

    println!("Facts server: http://{}/facts", listener.local_addr()?);

    // The launcher stops the cluster before stopping this HTTP service.
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = terminate.recv() => {},
            }
        })
        .await?;

    Ok(())
}
