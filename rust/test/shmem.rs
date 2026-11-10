#![cfg(target_os = "linux")]
use bytes::Bytes;
use monsterft::net::shmem::{ShmemEndpoint, ShmemOptions};
use std::{io, time::Duration};
fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
async fn pair(options: ShmemOptions) -> (ShmemEndpoint, ShmemEndpoint) {
    let (mut a, h) = ShmemEndpoint::pair(options.clone()).unwrap();
    let mut b = h.open(options).unwrap();
    tokio::try_join!(a.ready(), b.ready()).unwrap();
    (a, b)
}
#[test]
fn exact_frames_across_ring_wraps_and_large_then_small() {
    rt().block_on(async {
        let (mut a, mut b) = pair(ShmemOptions {
            ring_bytes: 4096,
            ..Default::default()
        })
        .await;
        let send = a.sender();
        let reply = b.sender();
        let sizes = [
            0,
            1,
            17,
            3970,
            4096,
            8192,
            1024 * 1024,
            13,
            8 * 1024 * 1024,
            1,
        ];
        tokio::time::timeout(Duration::from_secs(10), async {
            let server = async {
                for _ in sizes {
                    reply.send(b.recv().await.unwrap()).unwrap();
                }
                let _ = b.recv().await;
            };
            let client = async {
                for len in sizes {
                    let bytes = Bytes::from((0..len).map(|i| (i % 251) as u8).collect::<Vec<_>>());
                    send.send(bytes.clone()).unwrap();
                    assert_eq!(a.recv().await.unwrap(), bytes);
                    assert_eq!(send.queued_bytes(), 0);
                }
            };
            tokio::select! {_=client=>{},_=server=>panic!("server ended")}
        })
        .await
        .unwrap();
    });
}
#[test]
fn simultaneous_large_writes_progress_both_directions() {
    rt().block_on(async {
        let (mut a, mut b) = pair(ShmemOptions {
            ring_bytes: 4096,
            ..Default::default()
        })
        .await;
        let x = Bytes::from(vec![23; 2 * 1024 * 1024]);
        let y = Bytes::from(vec![43; 2 * 1024 * 1024]);
        a.sender().send(x.clone()).unwrap();
        b.sender().send(y.clone()).unwrap();
        let (at, bt) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::try_join!(a.recv(), b.recv())
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(at, y);
        assert_eq!(bt, x);
    });
}
#[test]
fn cancelled_reads_retain_partial_frames_and_fifo_order() {
    rt().block_on(async {
        let (mut a, mut b) = pair(ShmemOptions {
            ring_bytes: 4096,
            ..Default::default()
        })
        .await;
        let a_send = a.sender();
        let b_send = b.sender();
        let payload = Bytes::from(vec![91; 1024 * 1024]);
        a_send.send(payload.clone()).unwrap();
        a_send.send(Bytes::from_static(b"second")).unwrap();
        // Publish only the first ringful, then cancel; receiver cannot yet finish.
        assert!(
            tokio::time::timeout(Duration::from_millis(2), a.recv())
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(2), b.recv())
                .await
                .is_err()
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            let server = async {
                assert_eq!(b.recv().await.unwrap(), payload);
                assert_eq!(b.recv().await.unwrap(), Bytes::from_static(b"second"));
                b_send.send(Bytes::from_static(b"ok")).unwrap();
                let _ = b.recv().await;
            };
            let client = async {
                assert_eq!(a.recv().await.unwrap(), Bytes::from_static(b"ok"));
            };
            tokio::select! {_=client=>{},_=server=>panic!("server ended")}
        })
        .await
        .unwrap();
    });
}
#[test]
fn overflow_size_limits_and_stop_are_terminal() {
    rt().block_on(async {
        let limits = ShmemOptions {
            max_frame_bytes: 16,
            max_queued_bytes: 32,
            max_queued_messages: 2,
            ..Default::default()
        };
        let (a, h) = ShmemEndpoint::pair(limits.clone()).unwrap();
        let send = a.sender();
        send.send(Bytes::from(vec![1; 16])).unwrap();
        send.send(Bytes::from(vec![2; 16])).unwrap();
        assert!(send.send(Bytes::new()).is_err());
        assert!(send.is_stopped());
        assert_eq!(send.queued_bytes(), 0);
        drop(h);
        let (a, _) = ShmemEndpoint::pair(limits).unwrap();
        assert!(a.sender().send(Bytes::from(vec![1; 17])).is_err());
        assert!(a.sender().is_stopped());
    });
}
#[test]
fn dropped_peer_and_stop_wake_readiness_and_receive() {
    rt().block_on(async {
        let (mut a, h) = ShmemEndpoint::pair(ShmemOptions::default()).unwrap();
        drop(h);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), a.ready())
                .await
                .unwrap()
                .is_err()
        );
        let (mut a, b) = pair(ShmemOptions::default()).await;
        drop(b);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), a.recv())
                .await
                .unwrap()
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        let (mut a, _b) = pair(ShmemOptions::default()).await;
        let stop = a.sender();
        let stop_task = async {
            tokio::time::sleep(Duration::from_millis(2)).await;
            stop.stop();
        };
        let (result, ()) = tokio::join!(
            tokio::time::timeout(Duration::from_secs(1), a.recv()),
            stop_task
        );
        assert!(result.unwrap().is_err());
    });
}
#[test]
fn corrupt_length_rejected_before_payload_allocation() {
    rt().block_on(async {
        use shmem_ipc::sharedring;
        use std::{fs::File, io::Write, os::unix::net::UnixStream};
        let options = ShmemOptions {
            max_frame_bytes: 16,
            max_queued_bytes: 32,
            ..Default::default()
        };
        let (mut endpoint, handles) = ShmemEndpoint::pair(options.clone()).unwrap();
        let [sm, se, sf, _rm, _re, _rf, control] = handles.0;
        let mut raw = sharedring::Sender::<u8>::open(
            options.ring_bytes,
            File::from(sm),
            File::from(se),
            File::from(sf),
        )
        .unwrap();
        let mut control = UnixStream::from(control);
        control.write_all(&[1]).unwrap();
        endpoint.ready().await.unwrap();
        let length = 17u32.to_be_bytes();
        raw.send_raw(|p, n| {
            assert!(n >= 4);
            unsafe {
                std::ptr::copy_nonoverlapping(length.as_ptr(), p, 4);
            }
            4
        })
        .unwrap();
        assert_eq!(
            endpoint.recv().await.unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert!(endpoint.sender().is_stopped());
    });
}
#[test]
fn fresh_pairs_do_not_receive_previous_attempt_bytes() {
    rt().block_on(async {
        for _ in 0..20 {
            let (mut a, mut b) = pair(ShmemOptions::default()).await;
            a.sender().send(Bytes::from_static(b"old")).unwrap();
            let received = async {
                assert_eq!(b.recv().await.unwrap(), Bytes::from_static(b"old"));
            };
            tokio::select! {_=received=>{},r=a.recv()=>panic!("unexpected {r:?}")}
            drop(a);
            drop(b);
            let (mut fresh, _other) = pair(ShmemOptions::default()).await;
            assert!(
                tokio::time::timeout(Duration::from_millis(1), fresh.recv())
                    .await
                    .is_err()
            );
        }
    });
}

#[test]
fn capacity_disagreement_rejected_before_mapping() {
    rt().block_on(async {
        let (_endpoint, handles) = ShmemEndpoint::pair(ShmemOptions::default()).unwrap();
        assert!(
            handles
                .open(ShmemOptions {
                    ring_bytes: 4096,
                    ..Default::default()
                })
                .is_err()
        );
    });
}

// Run only in the subprocess created by real_child_exit_is_detected.
#[test]
#[ignore = "subprocess fixture"]
fn inherited_ring_child() {
    rt().block_on(async {
        use monsterft::net::shmem::ShmemHandles;
        // SAFETY: The parent test transfers these seven unique descriptors once.
        let handles = unsafe { ShmemHandles::from_inherited([4, 5, 6, 7, 8, 9, 10]) }.unwrap();
        let mut endpoint = handles.open(ShmemOptions::default()).unwrap();
        endpoint.ready().await.unwrap();
        let bytes = endpoint.recv().await.unwrap();
        endpoint.sender().send(bytes).unwrap();
        let _ = endpoint.recv().await;
    });
}

#[test]
fn real_child_exit_is_detected_and_descriptors_are_released() {
    use command_fds::{CommandFdExt, FdMapping};
    use std::process::{Child, Command};
    struct Reap(Child);
    impl Drop for Reap {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    rt().block_on(async {
        let (mut endpoint, handles) = ShmemEndpoint::pair(ShmemOptions::default()).unwrap();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command.args([
            "--exact",
            "inherited_ring_child",
            "--ignored",
            "--nocapture",
        ]);
        command
            .fd_mappings(
                handles
                    .0
                    .into_iter()
                    .enumerate()
                    .map(|(i, parent_fd)| FdMapping {
                        parent_fd,
                        child_fd: 4 + i as i32,
                    })
                    .collect(),
            )
            .unwrap();
        let mut child = Reap(command.spawn().unwrap());
        drop(command); // Only the child may retain its side of the lifetime socket.
        tokio::time::timeout(Duration::from_secs(5), endpoint.ready())
            .await
            .unwrap()
            .unwrap();
        endpoint
            .sender()
            .send(Bytes::from_static(b"real process"))
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), endpoint.recv())
                .await
                .unwrap()
                .unwrap(),
            Bytes::from_static(b"real process")
        );
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), endpoint.recv())
                .await
                .unwrap()
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        assert!(endpoint.sender().is_stopped());
    });
}
