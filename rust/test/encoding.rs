use bytes::Bytes;
use monsterft::{
    Value,
    error::{Code, Error},
    local::StartupRequest,
    log::LogEntry,
    protocol::{self, Entry, Execute, Outcome},
    storage::Receipt,
    wire,
};

#[test]
fn borrowed_outcome_encoding_preserves_tuple_bytes_and_digest() {
    let values = [
        Value::Nil,
        true.into(),
        (-129).into(),
        u64::MAX.into(),
        Value::F64(1.25),
        Value::Ext(7, vec![1, 2, 3]),
        Value::Map(vec![
            ("b".into(), Value::Binary(vec![31; 65536])),
            ("a".into(), Value::Array(vec!["text".into(), 0.into()])),
        ]),
    ];
    let mut outcomes: Vec<_> = values.into_iter().map(Outcome::Fulfilled).collect();
    for stack in [None, Some(String::new()), Some("shared stack".into())] {
        for code in [None, Some(Code::Other(-100)), Some(Code::RpcIllegal)] {
            outcomes.push(Outcome::Rejected(Error {
                message: "failure".into(),
                code,
                sql_code: Some(-2),
                stack: stack.clone(),
            }));
        }
    }
    // The original dynamic tuple representation is the byte-compatibility oracle.
    let original: Vec<_> = outcomes.iter().map(|o| o.to_value(true)).collect();
    let encoded = wire::encode_payload(&outcomes).unwrap();
    assert_eq!(encoded, wire::encode_payload(&original).unwrap());
    assert_eq!(
        wire::decode_payload::<Vec<Outcome>>(&encoded).unwrap(),
        outcomes
    );
    let without_stacks: Vec<_> = outcomes.iter().map(|o| o.to_value(false)).collect();
    let expected = wire::checksum(
        &wire::encode_payload(&(2, 10, serde_bytes::Bytes::new(b"patchset"), without_stacks))
            .unwrap(),
    );
    assert_eq!(
        protocol::digest_cmd(2, 10, b"patchset", &outcomes).unwrap(),
        expected
    );
    assert_eq!(
        wire::decode_payload::<Value>(&wire::encode_payload(&outcomes[6]).unwrap()).unwrap(),
        original[6]
    );
}

#[test]
fn binary_vectors_and_hashes_accept_both_wire_and_owned_value_paths() {
    for data in [vec![], vec![0, 255], vec![13; 65536]] {
        let entry = LogEntry {
            term: 2,
            entry: data,
        };
        let payload = wire::encode_payload(&entry).unwrap();
        assert_eq!(wire::decode_payload::<LogEntry>(&payload).unwrap(), entry);
        let value: Value = wire::decode_payload(&payload).unwrap();
        assert_eq!(rmpv::ext::from_value::<LogEntry>(value).unwrap(), entry);
    }
    let execute = Execute {
        cid: "call".into(),
        tick_count: 4,
        term: 1,
        seq: 2,
        entry_hash: [31; 16],
        items: Some(vec![Bytes::from_static(b"command")]),
        match_index: Some(vec![1, 1, 1]),
    };
    let data = wire::encode_payload(&execute).unwrap();
    assert_eq!(wire::decode_payload::<Execute>(&data).unwrap(), execute);
    assert_eq!(
        rmpv::ext::from_value::<Execute>(wire::decode_payload::<Value>(&data).unwrap()).unwrap(),
        execute
    );
    for receipt in [
        Receipt::empty(),
        Receipt {
            seq: 2,
            entry_hash: Some([1; 16]),
            local_digest: Some([2; 16]),
        },
    ] {
        let data = wire::encode_payload(&receipt).unwrap();
        assert_eq!(wire::decode_payload::<Receipt>(&data).unwrap(), receipt);
        assert_eq!(
            rmpv::ext::from_value::<Receipt>(wire::decode_payload::<Value>(&data).unwrap())
                .unwrap(),
            receipt
        );
    }
}

#[test]
fn binary_fields_remain_strict_about_types_and_lengths() {
    for bad in [
        Value::Array(vec![1.into(); 16]),
        "sixteen-letters!!".into(),
        Value::Nil,
        Value::Binary(vec![]),
        Value::Binary(vec![1; 15]),
        Value::Binary(vec![1; 17]),
    ] {
        let hello = Value::Map(vec![
            ("type".into(), "hello".into()),
            ("cid".into(), "call".into()),
            ("seed".into(), bad.clone()),
        ]);
        assert!(
            wire::decode_payload::<StartupRequest>(&wire::encode_payload(&hello).unwrap()).is_err()
        );
        assert!(rmpv::ext::from_value::<StartupRequest>(hello).is_err());
        if !bad.is_nil() {
            let receipt = Value::Map(vec![
                ("seq".into(), 2.into()),
                ("entry_hash".into(), bad.clone()),
                ("local_digest".into(), Value::Nil),
            ]);
            assert!(
                wire::decode_payload::<Receipt>(&wire::encode_payload(&receipt).unwrap()).is_err()
            );
            assert!(rmpv::ext::from_value::<Receipt>(receipt).is_err());
        }
    }
    for bad in [
        Value::Array(vec![0.into(), 255.into()]),
        "bytes".into(),
        Value::Nil,
    ] {
        let entry = Value::Map(vec![
            ("term".into(), 1.into()),
            ("entry".into(), bad.clone()),
        ]);
        assert!(wire::decode_payload::<LogEntry>(&wire::encode_payload(&entry).unwrap()).is_err());
        assert!(rmpv::ext::from_value::<LogEntry>(entry).is_err());
        let cmd = Value::Map(vec![
            ("type".into(), "cmd".into()),
            ("matchIndex".into(), Value::Array(vec![1.into(); 3])),
            ("items".into(), Value::Array(vec![bad])),
        ]);
        assert!(wire::decode_payload::<Entry>(&wire::encode_payload(&cmd).unwrap()).is_err());
        assert!(rmpv::ext::from_value::<Entry>(cmd).is_err());
    }
}
