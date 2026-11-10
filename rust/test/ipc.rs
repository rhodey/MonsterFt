use bytes::Bytes;
use monsterft::net::ipc::{IpcEndpoint, IpcNamespace, IpcOptions, cleanup_namespace};
use std::{process::Command, time::Duration};
fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
#[test]
fn lifeline_read_end_does_not_keep_parent_alive() {
    use monsterft::net::lifeline::{LifetimePipe, ParentLifetime};
    rt().block_on(async {
        let pipe = LifetimePipe::new().unwrap();
        let child = ParentLifetime::new(pipe.child_reader().unwrap()).unwrap();
        let extra_read = pipe.child_reader().unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), child.closed())
                .await
                .is_err()
        );
        drop(pipe);
        tokio::time::timeout(Duration::from_secs(1), child.closed())
            .await
            .unwrap()
            .unwrap();
        drop(extra_read);
    });
}

#[test]
fn lifeline_child() {
    use monsterft::net::lifeline::ParentLifetime;
    if std::env::var_os("MFT_PIPE_CHILD").is_none() {
        return;
    }
    rt().block_on(async {
        // SAFETY: The parent mapped the sole inherited read descriptor to 3.
        let life = unsafe { ParentLifetime::from_inherited(3) }.unwrap();
        life.closed().await.unwrap();
    });
}

#[test]
fn real_child_observes_pipe_eof() {
    use command_fds::{CommandFdExt, FdMapping};
    use monsterft::net::lifeline::LifetimePipe;
    let pipe = LifetimePipe::new().unwrap();
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "lifeline_child", "--nocapture"])
        .env("MFT_PIPE_CHILD", "1");
    command
        .fd_mappings(vec![FdMapping {
            parent_fd: pipe.child_reader().unwrap(),
            child_fd: 3,
        }])
        .unwrap();
    let mut child = command.spawn().unwrap();
    drop(command);
    std::thread::sleep(Duration::from_millis(30));
    assert!(child.try_wait().unwrap().is_none());
    drop(pipe);
    let until = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        if std::time::Instant::now() >= until {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("inherited pipe failed to reach EOF");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn options(path: &std::path::Path) -> IpcOptions {
    IpcOptions {
        namespace: Some(IpcNamespace {
            directory: path.to_owned(),
            prefix: "mft_test_".into(),
        }),
        ..Default::default()
    }
}

#[test]
fn platform_endpoint_preserves_fifo_and_frame_bytes() {
    rt().block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let opts = options(directory.path());
        let (mut a, handles) = IpcEndpoint::parent("gate", "child", opts.clone()).unwrap();
        let mut b = IpcEndpoint::child("gate", "child", opts.clone(), handles).unwrap();
        tokio::try_join!(a.ready(), b.ready()).unwrap();
        let sizes = [0, 1, 65535, 65536, 65537, 2 * 1024 * 1024, 7];
        let client = async {
            for len in sizes {
                let bytes = Bytes::from((0..len).map(|i| (i % 251) as u8).collect::<Vec<_>>());
                a.sender().send(bytes.clone()).unwrap();
                assert_eq!(a.recv().await.unwrap(), bytes);
                assert_eq!(a.sender().queued_bytes(), 0);
            }
        };
        let server = async {
            for _ in sizes {
                let frame = b.recv().await.unwrap();
                b.sender().send(frame).unwrap();
            }
            let _ = b.recv().await;
            panic!("peer ended before client");
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! { _ = client => {}, _ = server => unreachable!() }
        })
        .await
        .unwrap();
        drop(a);
        drop(b);
        cleanup_namespace(opts.namespace.as_ref().unwrap()).unwrap();
    });
}

#[test]
fn platform_endpoint_size_limits_and_stop() {
    rt().block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let mut opts = options(directory.path());
        opts.max_frame_bytes = 8;
        opts.max_queued_bytes = 16;
        opts.max_queued_messages = 2;
        let (mut endpoint, handles) = IpcEndpoint::parent("gate", "child", opts.clone()).unwrap();
        let sender = endpoint.sender();
        sender.send(Bytes::from_static(b"12345678")).unwrap();
        sender.send(Bytes::from_static(b"abcdefgh")).unwrap();
        assert!(sender.send(Bytes::new()).is_err());
        assert!(sender.is_stopped());
        assert!(endpoint.ready().await.is_err());
        assert!(sender.send(Bytes::new()).is_err());
        drop(handles);
        drop(endpoint);
        cleanup_namespace(opts.namespace.as_ref().unwrap()).unwrap();
    });
}

#[test]
#[ignore = "subprocess fixture"]
fn platform_child() {
    rt().block_on(async {
        let opts = options(std::path::Path::new(
            &std::env::var("MFT_PLATFORM_IPC_DIR").unwrap(),
        ));
        // SAFETY: The parent test maps exactly the descriptors returned by parent().
        let mut endpoint =
            unsafe { IpcEndpoint::from_inherited("gate", "child", opts, 4) }.unwrap();
        endpoint.ready().await.unwrap();
        let frame = endpoint.recv().await.unwrap();
        endpoint.sender().send(frame).unwrap();
        assert_eq!(endpoint.recv().await.unwrap(), Bytes::from_static(b"exit"));
    });
}

#[test]
fn platform_endpoint_inherited_by_real_child() {
    use command_fds::{CommandFdExt, FdMapping};
    rt().block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let opts = options(directory.path());
        let (mut endpoint, handles) = IpcEndpoint::parent("gate", "child", opts.clone()).unwrap();
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "platform_child", "--ignored", "--nocapture"])
            .env("MFT_PLATFORM_IPC_DIR", directory.path());
        command
            .fd_mappings(
                handles
                    .into_iter()
                    .enumerate()
                    .map(|(i, parent_fd)| FdMapping {
                        parent_fd,
                        child_fd: 4 + i as i32,
                    })
                    .collect(),
            )
            .unwrap();
        let mut child = command.spawn().unwrap();
        drop(command);
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            endpoint.ready().await?;
            let frame = Bytes::from(vec![79; 1024 * 1024]);
            endpoint.sender().send(frame.clone())?;
            assert_eq!(endpoint.recv().await?, frame);
            endpoint.sender().send(Bytes::from_static(b"exit"))?;
            let _ = tokio::time::timeout(Duration::from_millis(100), endpoint.recv()).await;
            loop {
                if let Some(status) = child.try_wait()? {
                    assert!(status.success());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Ok::<_, std::io::Error>(())
        })
        .await;
        if result.is_err() || matches!(&result, Ok(Err(_))) {
            let _ = child.kill();
            let _ = child.wait();
        }
        result.unwrap().unwrap();
        drop(endpoint);
        cleanup_namespace(opts.namespace.as_ref().unwrap()).unwrap();
    });
}
