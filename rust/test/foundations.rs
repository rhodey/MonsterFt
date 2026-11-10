use monsterft::{
    error::{Code, Error, Result},
    executor::Executor,
    random::Random,
    wire,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Envelope {
    #[serde(rename = "type")]
    kind: String,
    #[serde(with = "serde_bytes")]
    data: Vec<u8>,
}

#[test]
fn wire_reference_vectors_and_original_bytes() -> Result<()> {
    assert_eq!(
        wire::checksum(b""),
        [
            0x99, 0xaa, 0x06, 0xd3, 0x01, 0x47, 0x98, 0xd8, 0x60, 0x01, 0xc3, 0x24, 0x46, 0x8d,
            0x49, 0x7f
        ]
    );
    assert_eq!(
        wire::checksum(b"abc"),
        [
            0x06, 0xb0, 0x5a, 0xb6, 0x73, 0x3a, 0x61, 0x85, 0x78, 0xaf, 0x5f, 0x94, 0x89, 0x2f,
            0x39, 0x50
        ]
    );
    let value = Envelope {
        kind: "cmd".into(),
        data: vec![0, 255],
    };
    let encoded = wire::encode_payload(&value)?;
    assert_eq!(
        encoded,
        [
            0x82, 0xa4, b't', b'y', b'p', b'e', 0xa3, b'c', b'm', b'd', 0xa4, b'd', b'a', b't',
            b'a', 0xc4, 2, 0, 255
        ]
    );
    let frame = wire::encode_frame(&value)?;
    assert_eq!(wire::verify_frame(&frame)?, encoded);
    assert_eq!(wire::decode_frame::<Envelope>(&frame)?, value);
    let assembled = wire::frame_payload(&encoded, &wire::checksum(&encoded))?;
    assert_eq!(assembled, frame);
    let wrong = wire::frame_payload(&encoded, &[1; 16])?;
    assert_eq!(&wrong[..16], &[1; 16]);
    assert_eq!(
        wire::verify_frame(&wrong).unwrap_err().code,
        Some(Code::FrameCorrupt)
    );
    Ok(())
}

#[test]
fn decoding_rejects_corruption_trailing_and_deep_values() -> Result<()> {
    let mut frame = wire::encode_frame(&42)?.to_vec();
    frame[16] ^= 1;
    assert!(wire::decode_frame::<i64>(&frame).is_err());
    assert!(wire::decode_payload::<i64>(&[42, 43]).is_err());
    assert!(wire::decode_payload::<rmpv::Value>(&[0xc1]).is_err());
    assert!(wire::decode_payload::<rmpv::Value>(&[0xc6, 255, 255, 255, 255]).is_err());
    assert!(wire::decode_payload::<rmpv::Value>(&[0xdd, 255, 255, 255, 255]).is_err());
    let mut deep = vec![0x91; wire::MAX_DEPTH + 1];
    deep.push(0xc0);
    assert!(wire::decode_payload::<rmpv::Value>(&deep).is_err());
    assert!(wire::verify_frame(&[0; 16]).is_err());
    Ok(())
}

#[test]
fn opaque_map_order_shared_stacks_and_unknown_error_codes_survive() -> Result<()> {
    let map = rmpv::Value::Map(vec![("b".into(), 2.into()), ("a".into(), 1.into())]);
    assert_eq!(
        wire::decode_payload::<rmpv::Value>(&wire::encode_payload(&map)?)?,
        map
    );
    let mut error = Error::application("application rejected");
    error.code = Some(Code::Other(27));
    error.sql_code = Some(123);
    error.stack = Some("shared callback stack".into());
    let encoded = wire::encode_payload(&error)?;
    assert_eq!(wire::decode_payload::<Error>(&encoded)?, error);
    let fields: rmpv::Value = wire::decode_payload(&encoded)?;
    assert_eq!(fields.as_map().unwrap()[1].1.as_i64(), Some(27));
    let sqlite = rusqlite::Connection::open_in_memory()?
        .execute("select nonexistent", [])
        .unwrap_err();
    let error = Error::from(sqlite);
    assert_eq!(error.code, Some(Code::SqliteError));
    assert!(error.sql_code.is_some());
    assert!(error.stack.is_none());
    Ok(())
}

#[test]
fn random_reference_vector_and_full_state() -> Result<()> {
    let seed = [1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0];
    let mut random = Random::new(seed)?;
    assert_eq!(random.cid(), "03000000000000000300030160000000");
    assert_eq!(random.int(0, u64::MAX)?, 2_360_170_716_294_286_339);
    assert_eq!(random.int(10, 20)?, 15);
    let mut a = Random::new(seed)?;
    let mut b = Random::new(seed)?;
    for _ in 0..1000 {
        assert_eq!(a.cid(), b.cid());
        assert_eq!(a.int(0, 101)?, b.int(0, 101)?);
    }
    assert!(Random::new([0; 16]).is_err());
    assert!(a.int(2, 1).is_err());
    assert_eq!(a.int(7, 7)?, 7);
    let mut changed = seed;
    changed[15] = 1;
    assert_ne!(Random::new(seed)?.cid(), Random::new(changed)?.cid());
    Ok(())
}

#[derive(Clone, Debug)]
enum Task {
    Name(&'static str),
    Input,
    Continue,
    Cancel(u64),
    Fail,
    AddTimer,
}

#[test]
fn executor_input_continuations_precede_due_timers_and_finalization() -> Result<()> {
    let mut executor = Executor::new();
    let mut events = Vec::new();
    executor.after(20, Task::Name("later"), 0)?;
    executor.after(10, Task::Name("first"), 0)?;
    executor.after(10, Task::Name("second"), 0)?;
    executor.run(
        30,
        Task::Input,
        &mut events,
        |events, executor, task| {
            match task {
                Task::Input => {
                    events.push("input");
                    executor.enqueue(Task::Continue);
                }
                Task::Continue => events.push("continuation"),
                Task::Name(name) => events.push(name),
                _ => unreachable!(),
            }
            Ok(())
        },
        |events, executor| {
            assert!(executor.is_running());
            events.push("complete");
            Ok(())
        },
    )?;
    assert_eq!(
        events,
        [
            "input",
            "continuation",
            "first",
            "second",
            "later",
            "complete"
        ]
    );
    assert!(!executor.is_running());
    Ok(())
}

#[test]
fn executor_cancellation_repeats_new_due_timers_and_equal_time() -> Result<()> {
    let mut executor = Executor::new();
    let mut events = Vec::new();
    let canceled = executor.after(10, Task::Name("canceled"), 0)?;
    executor.after(10, Task::Name("repeat"), 10)?;
    fn dispatch(
        events: &mut Vec<&'static str>,
        executor: &mut Executor<Task>,
        task: Task,
    ) -> Result<()> {
        match task {
            Task::Name(name) => events.push(name),
            Task::Cancel(id) => executor.cancel(id),
            Task::AddTimer => {
                executor.after(0, Task::Name("new timer"), 0)?;
            }
            _ => {}
        }
        Ok(())
    }
    executor.run(
        100,
        Task::Cancel(canceled),
        &mut events,
        dispatch,
        |_, _| Ok(()),
    )?;
    assert_eq!(events, ["repeat"]);
    executor.run(100, Task::Input, &mut events, dispatch, |_, _| Ok(()))?;
    assert_eq!(events, ["repeat"]);
    executor.run(109, Task::AddTimer, &mut events, dispatch, |_, _| Ok(()))?;
    assert_eq!(events, ["repeat", "new timer"]);
    executor.run(110, Task::Input, &mut events, dispatch, |_, _| Ok(()))?;
    assert_eq!(events, ["repeat", "new timer", "repeat"]);
    executor.clear();
    executor.run(500, Task::Input, &mut events, dispatch, |_, _| Ok(()))?;
    assert_eq!(events.len(), 3);
    Ok(())
}

#[test]
fn executor_failed_input_skips_completion_and_clears_runnable_work() -> Result<()> {
    let mut executor = Executor::new();
    let mut completed = false;
    let result = executor.run(
        10,
        Task::Fail,
        &mut completed,
        |_, executor, _| {
            executor.enqueue(Task::Continue);
            Err(Error::new(Code::MonsterIllegal, "input failed"))
        },
        |completed, _| {
            *completed = true;
            Ok(())
        },
    );
    assert!(result.is_err());
    assert!(!completed);
    assert!(!executor.is_running());
    assert!(
        executor
            .run(9, Task::Input, &mut (), |_, _, _| Ok(()), |_, _| Ok(()))
            .is_err()
    );
    let mut count = 0;
    executor.run(
        10,
        Task::Input,
        &mut count,
        |count, _, _| {
            *count += 1;
            Ok(())
        },
        |_, _| Ok(()),
    )?;
    assert_eq!(count, 1);
    assert!(executor.after(u64::MAX, Task::Input, 0).is_err());
    Ok(())
}
