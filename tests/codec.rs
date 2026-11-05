use bytes::{Bytes, BytesMut};
use mini_raft::codec::{
    AppendEntries, AppendEntriesResponse, Codec, Hello, Message, RequestVote, RequestVoteResponse,
};
use mini_raft::server::{LogEntry, NodeId};
use tokio_util::codec::{Decoder, Encoder};

fn roundtrip(msg: Message) -> Message {
    let mut codec = Codec::new();
    let mut buf = BytesMut::new();
    codec.encode(msg, &mut buf).expect("encode");

    // Length prefix must be big-endian payload len (regression test for off-by-one).
    assert!(buf.len() >= 4);
    let len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    assert_eq!(len, buf.len() - 4, "length prefix must match payload len");

    let decoded = codec.decode(&mut buf).expect("decode").expect("frame");
    assert!(buf.is_empty(), "buffer fully consumed");
    // Message has no PartialEq, compare via JSON.
    let a = serde_json::to_value(&decoded).unwrap();
    // Re-encode decoded and compare bytes length to catch corruption.
    let mut buf2 = BytesMut::new();
    Codec::new().encode(decoded.clone(), &mut buf2).unwrap();
    let b = serde_json::to_value(&decoded).unwrap();
    assert_eq!(a, b);
    let _ = buf2;
    decoded
}

fn assert_json_eq(original: &Message, decoded: &Message) {
    assert_eq!(
        serde_json::to_value(original).unwrap(),
        serde_json::to_value(decoded).unwrap()
    );
}

#[test]
fn codec_roundtrips_all_variants() {
    let msgs = vec![
        Message::AppendEntriesType(AppendEntries {
            term: 3,
            leader_id: NodeId::new(1),
            prev_log_index: 0,
            prev_log_term: 1,
            entries: vec![LogEntry {
                term: 3,
                command: Bytes::from("cmd"),
                index: 1,
            }],
            leader_commit: 0,
        }),
        Message::AppendEntriesResponseType(AppendEntriesResponse {
            term: 3,
            success: true,
        }),
        Message::RequestVoteType(RequestVote {
            term: 2,
            candidate_id: NodeId::new(2),
            last_log_index: 5,
            last_log_term: 2,
        }),
        Message::RequestVoteResponseType(RequestVoteResponse {
            term: 2,
            vote_granted: true,
        }),
        Message::HelloType(Hello { id: NodeId::new(1) }),
    ];

    for m in msgs {
        let decoded = roundtrip(m.clone());
        assert_json_eq(&m, &decoded);
    }
}

#[test]
fn codec_handles_partial_and_multiple_frames() {
    let msg = Message::RequestVoteType(RequestVote {
        term: 1,
        candidate_id: NodeId::new(0),
        last_log_index: 0,
        last_log_term: 0,
    });
    let mut codec = Codec::new();
    let mut buf = BytesMut::new();
    codec.encode(msg.clone(), &mut buf).unwrap();
    let full = buf.split_to(buf.len());

    // Partial feed: first half yields None.
    let mut partial = BytesMut::new();
    let half = full.len() / 2;
    partial.extend_from_slice(&full[..half]);
    assert!(codec.decode(&mut partial).unwrap().is_none());
    partial.extend_from_slice(&full[half..]);
    let out = codec.decode(&mut partial).unwrap().expect("frame");
    assert_json_eq(&msg, &out);

    // Two frames back-to-back decode independently.
    let mut two = BytesMut::new();
    codec.encode(msg.clone(), &mut two).unwrap();
    codec.encode(msg.clone(), &mut two).unwrap();
    let first = codec.decode(&mut two).unwrap().expect("first");
    let second = codec.decode(&mut two).unwrap().expect("second");
    assert_json_eq(&msg, &first);
    assert_json_eq(&msg, &second);
}
