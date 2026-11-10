#![cfg(target_os = "macos")]
use bytes::Bytes;
use monsterft::net::iceoryx::{IpcEndpoint, IpcOptions};
use std::{
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn names() -> (String, String) {
    // Concurrent calls can observe the same clock value, even with as_nanos().
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = format!(
        "mft-test-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    (format!("{id}/a"), format!("{id}/b"))
}
fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn bytes_roundtrip_and_notifications() {
    rt().block_on(async {
        let (an, bn) = names();
        let mut a = IpcEndpoint::open(&an, &bn, IpcOptions::default()).unwrap();
        let mut b = IpcEndpoint::open(&bn, &an, IpcOptions::default()).unwrap();
        tokio::try_join!(a.ready(), b.ready()).unwrap();
        let a_send = a.sender();
        let b_send = b.sender();
        let sizes = [0, 17, 4096, 1024 * 1024, 13, 8 * 1024 * 1024, 1];
        let echo = async {
            for _ in sizes {
                let bytes = b.recv().await.unwrap();
                b_send.send(bytes).unwrap();
            }
            // Drive the last queued response before waiting for one final message.
            assert_eq!(b.recv().await.unwrap(), Bytes::from_static(b"done"));
        };
        let client = async {
            for len in sizes {
                let bytes = Bytes::from((0..len).map(|i| (i % 251) as u8).collect::<Vec<_>>());
                a_send.send(bytes.clone()).unwrap();
                let got = a.recv().await.unwrap();
                assert_eq!(got, bytes);
                assert_eq!(a_send.queued_bytes(), 0);
            }
            a_send.send(Bytes::from_static(b"done")).unwrap();
            // The echo completes after publication while this future waits for a response.
            let _ = a.recv().await;
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! { _ = echo => {}, _ = client => panic!("client should await termination") }
        }).await.unwrap();
    });
}

#[test]
fn overflow_and_stop_are_terminal() {
    rt().block_on(async {
        let (an, bn) = names();
        let options = IpcOptions {
            namespace: None,
            max_frame_bytes: 16,
            max_queued_bytes: 32,
            max_queued_messages: 2,
        };
        let a = IpcEndpoint::open(&an, &bn, options).unwrap();
        let sender = a.sender();
        sender.send(Bytes::from(vec![1; 16])).unwrap();
        sender.send(Bytes::from(vec![2; 16])).unwrap();
        assert!(sender.send(Bytes::new()).is_err());
        assert!(sender.is_stopped());
        assert!(sender.send(Bytes::new()).is_err());
    });
}

#[test]
fn missing_subscriber_does_not_accept_delivery() {
    rt().block_on(async {
        let (an, bn) = names();
        let mut a = IpcEndpoint::open(&an, &bn, IpcOptions::default()).unwrap();
        let mut b = IpcEndpoint::open(&bn, &an, IpcOptions::default()).unwrap();
        tokio::try_join!(a.ready(), b.ready()).unwrap();
        drop(b);
        a.sender().send(Bytes::from_static(b"must fail")).unwrap();
        assert!(a.recv().await.is_err());
        assert!(a.sender().is_stopped());
    });
}

#[test]
fn child_process_echo() {
    let Ok(an) = std::env::var("MFT_IPC_CHILD_A") else {
        return;
    };
    let bn = std::env::var("MFT_IPC_CHILD_B").unwrap();
    rt().block_on(async {
        let mut endpoint = IpcEndpoint::open(&bn, &an, IpcOptions::default()).unwrap();
        endpoint.ready().await.unwrap();
        let bytes = endpoint.recv().await.unwrap();
        endpoint.sender().send(bytes).unwrap();
        assert_eq!(endpoint.recv().await.unwrap(), Bytes::from_static(b"exit"));
    });
}

#[test]
fn real_child_process_roundtrip() {
    rt().block_on(async {
        let (an, bn) = names();
        let mut endpoint = IpcEndpoint::open(&an, &bn, IpcOptions::default()).unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "child_process_echo", "--nocapture"])
            .env("MFT_IPC_CHILD_A", &an)
            .env("MFT_IPC_CHILD_B", &bn)
            .spawn()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), endpoint.ready())
            .await
            .unwrap()
            .unwrap();
        let payload = Bytes::from(vec![73; 2 * 1024 * 1024]);
        endpoint.sender().send(payload.clone()).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), endpoint.recv())
                .await
                .unwrap()
                .unwrap(),
            payload
        );
        endpoint.sender().send(Bytes::from_static(b"exit")).unwrap();
        // Publish then allow the child to exit, without requiring a response.
        let _ = tokio::time::timeout(Duration::from_millis(100), endpoint.recv()).await;
        assert!(child.wait().unwrap().success());
    });
}

#[test]
fn shared_memory_queue_does_not_overwrite() {
    rt().block_on(async {
        let (an, bn) = names();
        let mut a = IpcEndpoint::open(&an, &bn, IpcOptions::default()).unwrap();
        let mut b = IpcEndpoint::open(&bn, &an, IpcOptions::default()).unwrap();
        tokio::try_join!(a.ready(), b.ready()).unwrap();
        for n in 1..=3 {
            a.sender().send(Bytes::from(vec![n; 16])).unwrap();
        }
        assert!(a.recv().await.is_err());
        assert_eq!(b.recv().await.unwrap(), Bytes::from(vec![1; 16]));
        assert_eq!(b.recv().await.unwrap(), Bytes::from(vec![2; 16]));
    });
}

#[test]
fn namespace_child() {
    use monsterft::net::iceoryx::IpcNamespace;
    let Ok(directory) = std::env::var("MFT_NAMESPACE_DIR") else {
        return;
    };
    let prefix = std::env::var("MFT_NAMESPACE_PREFIX").unwrap();
    let an = std::env::var("MFT_NAMESPACE_A").unwrap();
    let bn = std::env::var("MFT_NAMESPACE_B").unwrap();
    rt().block_on(async {
        let options = IpcOptions {
            namespace: Some(IpcNamespace {
                directory: directory.into(),
                prefix,
            }),
            ..IpcOptions::default()
        };
        let mut endpoint = IpcEndpoint::open(&bn, &an, options).unwrap();
        endpoint.ready().await.unwrap();
        let _ = endpoint.recv().await;
    });
}

#[test]
fn only_owned_namespace_is_cleaned_after_child_exit() {
    use monsterft::net::iceoryx::{IpcNamespace, cleanup_namespace};
    rt().block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let namespace = IpcNamespace {
            directory: directory.path().to_owned(),
            prefix: format!("mft{}x", std::process::id()),
        };
        let options = IpcOptions {
            namespace: Some(namespace.clone()),
            ..IpcOptions::default()
        };
        let (an, bn) = names();
        let mut endpoint = IpcEndpoint::open(&an, &bn, options).unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "namespace_child", "--nocapture"])
            .env("MFT_NAMESPACE_DIR", directory.path())
            .env("MFT_NAMESPACE_PREFIX", &namespace.prefix)
            .env("MFT_NAMESPACE_A", &an)
            .env("MFT_NAMESPACE_B", &bn)
            .spawn()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), endpoint.ready())
            .await
            .unwrap()
            .unwrap();
        assert!(cleanup_namespace(&namespace).is_err());
        child.kill().unwrap();
        child.wait().unwrap();
        drop(endpoint);
        cleanup_namespace(&namespace).unwrap();
        cleanup_namespace(&namespace).unwrap();
    });
}
