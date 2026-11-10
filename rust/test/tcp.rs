use bytes::Bytes;
use monsterft::net::{
    tcp::{self, FrameOptions, FrameReader},
    tcpcluster::{TcpCluster, TcpOptions, TcpPeer, TcpSender},
};
use std::{
    cell::Cell,
    collections::BTreeMap,
    future::Future,
    io,
    net::{IpAddr, Ipv4Addr},
    rc::Rc,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

fn run(future: impl Future<Output = ()>) {
    static PORT_ALLOCATION: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = PORT_ALLOCATION.lock().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    tokio::task::LocalSet::new().block_on(&runtime, async {
        tokio::time::timeout(Duration::from_secs(10), future)
            .await
            .unwrap()
    });
}
fn options() -> TcpOptions {
    TcpOptions {
        bind_host: IpAddr::V4(Ipv4Addr::LOCALHOST),
        reconnect_delay: Duration::from_millis(10),
        connect_timeout: Duration::from_millis(200),
        ..TcpOptions::default()
    }
}
fn peers(ids: &[&str]) -> BTreeMap<String, TcpPeer> {
    let mut reservations = Vec::new();
    let mut result = BTreeMap::new();
    for id in ids {
        loop {
            let first = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
            let base = first.local_addr().unwrap().port();
            let mut group = vec![first];
            for index in 1..ids.len() {
                if let Some(port) = base.checked_add(index as u16)
                    && let Ok(listener) = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, port))
                {
                    group.push(listener);
                }
            }
            if group.len() == ids.len() {
                reservations.extend(group);
                result.insert(
                    (*id).into(),
                    TcpPeer {
                        host: "127.0.0.1".into(),
                        base_port: base,
                    },
                );
                break;
            }
        }
    }
    result
}
async fn connected(sender: &TcpSender, peers: &[&str]) {
    while !peers.iter().all(|peer| sender.connected(peer)) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[test]
fn framing_empty_fragmented_combined_and_cancelled_read() {
    run(async {
        let (mut writer, reader) = tokio::io::duplex(64);
        let mut reader = FrameReader::new(reader, 32).unwrap();
        writer.write_all(&[0, 0]).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(5), reader.recv())
                .await
                .is_err()
        );
        writer
            .write_all(&[0, 3, b'a', b'b', b'c', 0, 0, 0, 0, 0, 0, 0, 1, b'x'])
            .await
            .unwrap();
        assert_eq!(
            reader.recv().await.unwrap(),
            Some(Bytes::from_static(b"abc"))
        );
        assert_eq!(reader.recv().await.unwrap(), Some(Bytes::new()));
        assert_eq!(reader.recv().await.unwrap(), Some(Bytes::from_static(b"x")));
        writer.shutdown().await.unwrap();
        assert!(reader.recv().await.unwrap().is_none());
    });
}
#[test]
fn framing_checks_length_before_payload_and_detects_partial_eof() {
    run(async {
        let (mut writer, reader) = tokio::io::duplex(8);
        let mut reader = FrameReader::new(reader, 3).unwrap();
        writer.write_all(&4u32.to_be_bytes()).await.unwrap();
        assert_eq!(
            reader.recv().await.unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        let (mut writer, reader) = tokio::io::duplex(8);
        let mut reader = FrameReader::new(reader, 20).unwrap();
        writer.write_all(&[0, 0, 0, 5, 1, 2]).await.unwrap();
        writer.shutdown().await.unwrap();
        assert!(reader.recv().await.is_err());
    });
}
#[test]
fn partial_vectored_writes_preserve_exact_bytes() {
    run(async {
        let (mut writer, mut reader) = tokio::io::duplex(1);
        let frame = vec![29; 4096];
        let send = async {
            tcp::write_frame(&mut writer, &frame, 4096).await.unwrap();
            writer.shutdown().await.unwrap();
        };
        let recv = async {
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(&bytes[..4], &4096u32.to_be_bytes());
            assert_eq!(&bytes[4..], &frame);
        };
        tokio::join!(send, recv);
    });
}
#[test]
fn arbitrary_membership_routes_all_pairs_without_greetings() {
    run(async {
        let members = peers(&["zebra", "alpha", "middle"]);
        let mut a = TcpCluster::open("alpha", members.clone(), options())
            .await
            .unwrap();
        let mut m = TcpCluster::open("middle", members.clone(), options())
            .await
            .unwrap();
        let mut z = TcpCluster::open("zebra", members, options()).await.unwrap();
        connected(&a.sender(), &["middle", "zebra"]).await;
        connected(&m.sender(), &["alpha", "zebra"]).await;
        connected(&z.sender(), &["alpha", "middle"]).await;
        a.sender()
            .send("middle", Bytes::from_static(b"a-m"))
            .unwrap();
        m.sender()
            .send("zebra", Bytes::from_static(b"m-z"))
            .unwrap();
        z.sender().send("alpha", Bytes::new()).unwrap();
        assert_eq!(
            m.recv().await.unwrap(),
            Some(("alpha".into(), Bytes::from_static(b"a-m")))
        );
        assert_eq!(
            z.recv().await.unwrap(),
            Some(("middle".into(), Bytes::from_static(b"m-z")))
        );
        assert_eq!(
            a.recv().await.unwrap(),
            Some(("zebra".into(), Bytes::new()))
        );
        a.close().await.unwrap();
        m.close().await.unwrap();
        z.close().await.unwrap();
    });
}
#[test]
fn open_is_local_and_delayed_peer_reconnects() {
    run(async {
        let members = peers(&["a", "b"]);
        let mut a = TcpCluster::open("a", members.clone(), options())
            .await
            .unwrap();
        assert!(
            !a.sender()
                .send("b", Bytes::from_static(b"drop before connection"))
                .unwrap()
        );
        let mut b = TcpCluster::open("b", members.clone(), options())
            .await
            .unwrap();
        connected(&a.sender(), &["b"]).await;
        a.sender().send("b", Bytes::from_static(b"first")).unwrap();
        assert_eq!(
            b.recv().await.unwrap().unwrap().1,
            Bytes::from_static(b"first")
        );
        b.close().await.unwrap();
        while a.sender().connected("b") {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(
            !a.sender()
                .send("b", Bytes::from_static(b"drop between connections"))
                .unwrap()
        );
        let mut b = TcpCluster::open("b", members, options()).await.unwrap();
        connected(&a.sender(), &["b"]).await;
        a.sender().send("b", Bytes::from_static(b"second")).unwrap();
        assert_eq!(
            b.recv().await.unwrap().unwrap().1,
            Bytes::from_static(b"second")
        );
        a.close().await.unwrap();
        b.close().await.unwrap();
    });
}
#[test]
fn incoming_replacement_discards_stale_queued_frames() {
    run(async {
        let members = peers(&["a", "b"]);
        let address = (Ipv4Addr::LOCALHOST, members["b"].base_port);
        let mut b = TcpCluster::open("b", members, options()).await.unwrap();
        let mut old = TcpStream::connect(address).await.unwrap();
        tcp::write_frame(&mut old, b"old", 100).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        let mut new = TcpStream::connect(address).await.unwrap();
        tcp::write_frame(&mut new, b"new", 100).await.unwrap();
        let mut byte = [0];
        assert_eq!(old.read(&mut byte).await.unwrap(), 0);
        assert_eq!(
            b.recv().await.unwrap(),
            Some(("a".into(), Bytes::from_static(b"new")))
        );
        b.close().await.unwrap();
    });
}
#[test]
fn malformed_peer_warns_without_stopping_local_transport() {
    run(async {
        let members = peers(&["a", "b"]);
        let address = (Ipv4Addr::LOCALHOST, members["b"].base_port);
        let opts = TcpOptions {
            frames: FrameOptions {
                max_frame_bytes: 8,
                ..FrameOptions::default()
            },
            ..options()
        };
        let mut b = TcpCluster::open("b", members, opts).await.unwrap();
        let warned = Rc::new(Cell::new(false));
        let record = warned.clone();
        b.sender().on_warn(move |error| {
            if error.kind() == io::ErrorKind::InvalidData {
                record.set(true);
            }
        });
        let mut bad = TcpStream::connect(address).await.unwrap();
        bad.write_all(&9u32.to_be_bytes()).await.unwrap();
        let mut byte = [0];
        assert_eq!(bad.read(&mut byte).await.unwrap(), 0);
        while !warned.get() {
            tokio::task::yield_now().await;
        }
        assert!(b.is_open());
        let mut good = TcpStream::connect(address).await.unwrap();
        tcp::write_frame(&mut good, b"good", 8).await.unwrap();
        assert_eq!(
            b.recv().await.unwrap().unwrap().1,
            Bytes::from_static(b"good")
        );
        b.close().await.unwrap();
    });
}
#[test]
fn partial_listener_startup_releases_bound_ports() {
    run(async {
        let members = peers(&["a", "b", "c"]);
        let base = members["a"].base_port;
        let occupied = TcpListener::bind((Ipv4Addr::LOCALHOST, base + 2))
            .await
            .unwrap();
        assert!(TcpCluster::open("a", members, options()).await.is_err());
        let _first = TcpListener::bind((Ipv4Addr::LOCALHOST, base + 1))
            .await
            .unwrap();
        drop(occupied);
    });
}
#[test]
fn queue_budget_includes_prefix_until_write_completion() {
    run(async {
        let members = peers(&["a", "b"]);
        let mut opts = options();
        opts.frames.max_queued_bytes = 8;
        let mut a = TcpCluster::open("a", members.clone(), opts).await.unwrap();
        let mut b = TcpCluster::open("b", members, options()).await.unwrap();
        connected(&a.sender(), &["b"]).await;
        let sender = a.sender();
        assert!(sender.send("b", Bytes::from_static(b"1234")).unwrap());
        assert_eq!(sender.queued_bytes("b"), 8);
        assert!(!sender.send("b", Bytes::new()).unwrap());
        assert!(a.is_open());
        assert_eq!(
            b.recv().await.unwrap().unwrap().1,
            Bytes::from_static(b"1234")
        );
        while sender.queued_bytes("b") != 0 {
            tokio::task::yield_now().await;
        }
        assert!(sender.send("b", Bytes::new()).unwrap());
        assert!(b.recv().await.unwrap().unwrap().1.is_empty());
        a.close().await.unwrap();
        b.close().await.unwrap();
    });
}
#[test]
fn listener_panic_stops_helper_and_later_error_listeners_run() {
    run(async {
        let members = peers(&["a", "b"]);
        let mut opts = options();
        opts.frames.max_queued_bytes = 4;
        let mut a = TcpCluster::open("a", members.clone(), opts).await.unwrap();
        let mut b = TcpCluster::open("b", members, options()).await.unwrap();
        connected(&a.sender(), &["b"]).await;
        let sender = a.sender();
        let recorded = Rc::new(Cell::new(false));
        let seen = recorded.clone();
        sender.on_warn(|_| panic!("deliberate warning listener panic"));
        sender.on_error(|_| panic!("deliberate error listener panic"));
        sender.on_error(move |_| seen.set(true));
        assert!(
            !sender
                .send("b", Bytes::from_static(b"too big for queue"))
                .unwrap()
        );
        assert!(a.is_stopped());
        assert!(recorded.get());
        assert!(a.failure().await.to_string().contains("listener"));
        a.close().await.unwrap();
        a.close().await.unwrap();
        b.close().await.unwrap();
    });
}
#[test]
fn stop_cancels_dialing_without_reporting_terminal_failure() {
    run(async {
        let members = peers(&["a", "b"]);
        let mut a = TcpCluster::open("a", members, options()).await.unwrap();
        let failure = a.failure();
        a.stop();
        assert!(a.recv().await.unwrap().is_none());
        a.close().await.unwrap();
        a.close().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), failure)
                .await
                .is_err()
        );
        assert!(a.sender().send("b", Bytes::new()).is_err());
    });
}
#[test]
fn connection_carries_raw_length_then_payload() {
    run(async {
        let members = peers(&["a", "b"]);
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, members["b"].base_port))
            .await
            .unwrap();
        let mut a = TcpCluster::open("a", members, options()).await.unwrap();
        let (mut socket, _) = listener.accept().await.unwrap();
        connected(&a.sender(), &["b"]).await;
        let mut byte = [0];
        assert!(
            tokio::time::timeout(Duration::from_millis(10), socket.read(&mut byte))
                .await
                .is_err()
        );
        a.sender().send("b", Bytes::from_static(b"abc")).unwrap();
        let mut frame = [0; 7];
        socket.read_exact(&mut frame).await.unwrap();
        assert_eq!(&frame, b"\0\0\0\x03abc");
        a.close().await.unwrap();
    });
}

#[test]
fn dropped_close_waiter_keeps_cleanup_owned() {
    run(async {
        let members = peers(&["a", "b"]);
        let base = members["a"].base_port;
        let mut a = TcpCluster::open("a", members, options()).await.unwrap();
        drop(a.close());
        assert!(a.is_stopped());
        a.close().await.unwrap();
        let _listener = TcpListener::bind((Ipv4Addr::LOCALHOST, base + 1))
            .await
            .unwrap();
    });
}

#[test]
fn reconnect_never_replays_old_connection_queue() {
    run(async {
        let members = peers(&["a", "b"]);
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, members["b"].base_port))
            .await
            .unwrap();
        let mut a = TcpCluster::open("a", members, options()).await.unwrap();
        let (mut first, _) = listener.accept().await.unwrap();
        connected(&a.sender(), &["b"]).await;
        // A reverse frame is illegal on this one-way connection. Some old sends may
        // reach that old socket; none may appear on its replacement.
        tcp::write_frame(&mut first, b"reverse", 100).await.unwrap();
        for _ in 0..10 {
            let _ = a.sender().send("b", Bytes::from(vec![1; 4096])).unwrap();
        }
        let (mut replacement, _) = listener.accept().await.unwrap();
        connected(&a.sender(), &["b"]).await;
        let mut byte = [0];
        assert!(
            tokio::time::timeout(Duration::from_millis(20), replacement.read(&mut byte))
                .await
                .is_err()
        );
        a.sender().send("b", Bytes::from_static(b"new")).unwrap();
        let mut frame = [0; 7];
        replacement.read_exact(&mut frame).await.unwrap();
        assert_eq!(&frame, b"\0\0\0\x03new");
        a.close().await.unwrap();
    });
}
