use crate::server::{ClientId, LogEntry, NodeId};
use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use tokio::io;
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    AppendEntriesType(AppendEntries),
    AppendEntriesResponseType(AppendEntriesResponse),
    RequestVoteType(RequestVote),
    RequestVoteResponseType(RequestVoteResponse),
    ClientRequestType(ClientRequest),
    ClientResponseType(ClientResponse),
    HelloType(Hello),
    HelloClientType(HelloClient),
}

impl Message {
    pub fn message_type(&self) -> String {
        match self {
            Message::AppendEntriesType(_) => "AppendEntriesType".to_string(),
            Message::AppendEntriesResponseType(_) => "AppendEntriesResponseType".to_string(),
            Message::RequestVoteType(_) => "RequestVoteType".to_string(),
            Message::RequestVoteResponseType(_) => "RequestVoteResponseType".to_string(),
            Message::ClientRequestType(_) => "ClientRequestType".to_string(),
            Message::ClientResponseType(_) => "ClientResponseType".to_string(),
            Message::HelloType(_) => "HelloType".to_string(),
            Message::HelloClientType(_) => "HelloClientType".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntries {
    pub term: u32,
    pub leader_id: NodeId,
    pub prev_log_index: i32,
    pub prev_log_term: u32,
    pub entries: Vec<LogEntry>, // vec![] for heartbeat
    pub leader_commit: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u32,
    pub success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVote {
    pub term: u32,
    pub candidate_id: NodeId,
    pub last_log_index: i32,
    pub last_log_term: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: u32,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientRequest {
    pub command: bytes::Bytes,
    pub client_id: u32,
    pub request_id: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    pub id: NodeId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloClient {
    pub id: ClientId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientResponse {
    // Success responses for each operation
    Success { request_id: u64 },
    InternalError { request_id: u64, reason: String },
}

#[derive(Clone)]
pub struct Codec;

impl Codec {
    pub fn new() -> Self {
        Codec {}
    }
}

impl Encoder<Message> for Codec {
    type Error = io::Error;
    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let payload =
            serde_json::to_vec(&item).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        dst.put_u32(payload.len() as u32);
        dst.put_slice(&payload);
        Ok(())
    }
}

impl Decoder for Codec {
    type Item = Message;
    type Error = io::Error;
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // 4 byte length header
        if src.len() < 4 {
            //not enough bytes yet
            return Ok(None);
        }

        let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
        let frame_len = 4 + len;

        if src.len() < frame_len {
            src.reserve(frame_len - src.len());
            //not enough bytes yet
            return Ok(None);
        }

        let mut frame = src.split_to(frame_len);
        //skip header
        frame.advance(4);

        let msg: Message = serde_json::from_slice(&frame)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(Some(msg))
    }
}
