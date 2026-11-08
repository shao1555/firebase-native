//! The Write stream: the official protocol for committing
//! mutations, and the only write path when persistence is enabled.
//!
//! Protocol (per `firestore.proto` and the official clients):
//!
//! 1. The client opens the stream with a first request carrying
//!    only the database name (plus the stream id/token when
//!    resuming) and **no writes**, then waits: sends must not start
//!    until the server's first response arrives (the handshake).
//! 2. Each subsequent request carries one batch of writes and the
//!    latest stream token; each response acknowledges one batch
//!    with per-write update times and a commit time.
//! 3. When the stream breaks with a batch unacknowledged, the
//!    client reconnects and resumes with the last stream id/token.
//!    The server then replays the commit response if the batch
//!    landed (it must not be resent), and otherwise follows with
//!    the token-only handshake response, after which the batch is
//!    resent.
//! 4. A stream error whose code condemns the batch itself
//!    (precondition failed, invalid argument, not found, already
//!    exists, permission denied) is permanent for that batch:
//!    resending would fail identically, so it is returned to the
//!    caller instead of retried.
//!
//! Like the Listen side, the protocol is a pure state machine
//! ([`WriteProtocol`], unit-tested without I/O) driven by a thin
//! reconnect loop.

use std::collections::HashMap;
use std::sync::Arc;

use firebase_native_core::{Error, FirebaseOptions, Result, TokenProvider};
use tonic::transport::Channel;
use tonic::Request;

use crate::client::{build_channel, grpc_client, map_status};
use crate::google::firestore::v1::firestore_client::FirestoreClient;
use crate::google::firestore::v1::{Write, WriteRequest, WriteResponse};
use crate::listen::Backoff;
use crate::value::Timestamp;

/// Extracts per-write transform results from a commit
/// response, converting each to the public value model.
fn transform_results_of(
    results: &[crate::google::firestore::v1::WriteResult],
) -> Vec<Vec<crate::value::Value>> {
    results
        .iter()
        .map(|result| {
            result
                .transform_results
                .iter()
                .map(|value| crate::value::Value::from_proto(value.clone()))
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap_or_default()
        })
        .collect()
}

/// The acknowledgement of one committed batch.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CommittedBatch {
    /// Server commit time of the batch.
    pub(crate) commit_time: Option<Timestamp>,
    /// Per-write update times, in request order (`None` for
    /// deletes, which report no update time).
    pub(crate) update_times: Vec<Option<Timestamp>>,
    /// Per-write transform results, in request order: the
    /// server-computed values of each write's transformed
    /// fields, in transform order.
    pub(crate) transform_results: Vec<Vec<crate::value::Value>>,
}

/// What a response means for the protocol.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum WriteOutcome {
    /// The token-only response that completes the handshake on a
    /// (re)opened stream: batches may now be sent (or resent, when
    /// a batch is still outstanding and was not replayed).
    Handshake,
    /// The pending batch was committed (response on an established
    /// stream).
    BatchAck(CommittedBatch),
    /// A commit response the server replayed while resuming,
    /// before the handshake response: the batch it acknowledges
    /// was sent on the previous stream and must NOT be resent.
    ReplayedAck(CommittedBatch),
}

/// The Write stream protocol state machine: responses in, outcomes
/// out; owns the stream id/token across reconnections.
#[derive(Debug)]
pub(crate) struct WriteProtocol {
    database: String,
    stream_id: String,
    stream_token: Vec<u8>,
    handshaken: bool,
    pending_writes: Option<usize>,
}

impl WriteProtocol {
    pub(crate) fn new(database: String) -> Self {
        Self {
            database,
            stream_id: String::new(),
            stream_token: Vec::new(),
            handshaken: false,
            pending_writes: None,
        }
    }

    /// The first request on a new stream: database only, no writes.
    /// Carries the previous stream id/token when resuming.
    pub(crate) fn handshake_request(&self) -> WriteRequest {
        WriteRequest {
            database: self.database.clone(),
            stream_id: self.stream_id.clone(),
            writes: Vec::new(),
            stream_token: self.stream_token.clone(),
            labels: HashMap::new(),
            request_options: None,
        }
    }

    /// Builds the request for one batch and marks it pending.
    /// When the previous stream ended with this same batch still
    /// outstanding and the resume replay did not acknowledge it,
    /// the batch is resent: calling this with the outstanding
    /// batch (same write count) after the new handshake is the
    /// resend path, not a new batch.
    ///
    /// # Panics
    /// Panics if called before the handshake completed, or with a
    /// different batch while one is still unacknowledged
    /// (single-flight is a protocol requirement of this driver).
    pub(crate) fn batch_request(&mut self, writes: Vec<Write>) -> WriteRequest {
        assert!(self.handshaken, "batch requested before handshake");
        match self.pending_writes {
            None => self.pending_writes = Some(writes.len()),
            Some(pending) => {
                assert_eq!(
                    pending,
                    writes.len(),
                    "a different batch is still unacknowledged"
                );
            }
        }
        WriteRequest {
            database: String::new(),
            stream_id: String::new(),
            writes,
            stream_token: self.stream_token.clone(),
            labels: HashMap::new(),
            request_options: None,
        }
    }

    /// Feeds one response into the machine.
    pub(crate) fn on_response(&mut self, response: WriteResponse) -> Result<WriteOutcome> {
        if !response.stream_id.is_empty() {
            self.stream_id = response.stream_id.clone();
        }
        self.stream_token = response.stream_token.clone();
        if !self.handshaken && !response.write_results.is_empty() {
            // A response carrying results before the handshake can
            // only be a replayed commit on a resumed stream: it
            // acknowledges the batch left outstanding by the
            // previous stream. Consume it; the token-only handshake
            // response is still to come.
            let expected = self.pending_writes.take().ok_or_else(|| {
                Error::Other("replayed write results with no batch outstanding".to_string())
            })?;
            if response.write_results.len() != expected {
                return Err(Error::Other(format!(
                    "replayed write response carried {} results for a batch of {expected}",
                    response.write_results.len()
                )));
            }
            let transform_results = transform_results_of(&response.write_results);
            return Ok(WriteOutcome::ReplayedAck(CommittedBatch {
                commit_time: response.commit_time.map(Timestamp::from),
                update_times: response
                    .write_results
                    .into_iter()
                    .map(|result| result.update_time.map(Timestamp::from))
                    .collect(),
                transform_results,
            }));
        }
        if !self.handshaken {
            self.handshaken = true;
            return Ok(WriteOutcome::Handshake);
        }
        let expected = self.pending_writes.take().ok_or_else(|| {
            Error::Other("write response received with no batch pending".to_string())
        })?;
        if response.write_results.len() != expected {
            return Err(Error::Other(format!(
                "write response carried {} results for a batch of {expected}",
                response.write_results.len()
            )));
        }
        let transform_results = transform_results_of(&response.write_results);
        Ok(WriteOutcome::BatchAck(CommittedBatch {
            commit_time: response.commit_time.map(Timestamp::from),
            update_times: response
                .write_results
                .into_iter()
                .map(|result| result.update_time.map(Timestamp::from))
                .collect(),
            transform_results,
        }))
    }

    /// Notes that the stream ended. The outstanding batch, if
    /// any, is **preserved**: on resume the server replays the
    /// commit if it landed, and only a batch the replay passes
    /// over may be resent. Returns `true` when a batch is
    /// outstanding. The stream id/token survive for the resume.
    pub(crate) fn on_stream_end(&mut self) -> bool {
        self.handshaken = false;
        self.pending_writes.is_some()
    }

    /// Drops the outstanding batch without an acknowledgement
    /// (the caller condemned it after a permanent error).
    pub(crate) fn abandon_pending(&mut self) {
        self.pending_writes = None;
    }

    /// Whether a batch is currently unacknowledged.
    #[cfg(test)]
    pub(crate) fn has_pending(&self) -> bool {
        self.pending_writes.is_some()
    }
}

/// Whether a stream error condemns the pending batch itself, as
/// opposed to the connection. Such batches are dropped by the
/// engine rather than resent forever.
pub(crate) fn is_permanent_batch_error(error: &Error) -> bool {
    match error {
        Error::NotFound(_) | Error::PermissionDenied(_) | Error::InvalidArgument(_) => true,
        Error::Rpc { code, .. } => matches!(
            code.as_str(),
            "FailedPrecondition" | "AlreadyExists" | "OutOfRange"
        ),
        _ => false,
    }
}

/// Which phase of an attempt failed. The distinction matters: a
/// batch can only be condemned by the server after it was sent, so
/// permanent-error classification applies to the batch phase only.
enum AttemptFailure {
    /// Establishing (or re-establishing) the stream failed.
    Handshake(Error),
    /// The batch was sent and the stream failed before its ack.
    Batch(Error),
}

/// State salvaged from a dead stream: the protocol (stream
/// id/token and any outstanding batch) *and* a replayed
/// acknowledgement the protocol has already consumed. The two
/// must travel together: once a replay is consumed, the protocol
/// state no longer records that the batch committed, so dropping
/// the acknowledgement on a second disconnect would lose the
/// commit and resend the batch.
struct SalvagedState {
    protocol: WriteProtocol,
    replayed_ack: Option<CommittedBatch>,
}

struct Session {
    protocol: WriteProtocol,
    sender: tokio::sync::mpsc::Sender<WriteRequest>,
    responses: tonic::Streaming<WriteResponse>,
    /// A commit the server replayed during the resume handshake:
    /// it acknowledges the batch the current `write_batch` call is
    /// waiting on, which must therefore not be sent again.
    replayed_ack: Option<CommittedBatch>,
}

/// A single-flight Write stream client: reconnects and resends
/// internally, so `write_batch` returns only on commit or on a
/// permanent batch error.
pub(crate) struct WriteStream {
    options: FirebaseOptions,
    tokens: Arc<dyn TokenProvider>,
    /// The shared gRPC channel cell (the same one the owning
    /// `Firestore` uses), initialized on first connect so mere
    /// construction never performs I/O.
    channel_cell: Arc<tokio::sync::OnceCell<Channel>>,
    endpoint: String,
    session: tokio::sync::Mutex<Option<Session>>,
    /// State salvaged from a dead session, so the next session
    /// resumes the same server stream without losing a consumed
    /// replay.
    salvaged: std::sync::Mutex<Option<SalvagedState>>,
}

impl WriteStream {
    pub(crate) fn new(
        options: FirebaseOptions,
        tokens: Arc<dyn TokenProvider>,
        channel_cell: Arc<tokio::sync::OnceCell<Channel>>,
        endpoint: String,
    ) -> Self {
        Self {
            options,
            tokens,
            channel_cell,
            endpoint,
            session: tokio::sync::Mutex::new(None),
            salvaged: std::sync::Mutex::new(None),
        }
    }

    async fn channel(&self) -> Result<Channel> {
        let channel = self
            .channel_cell
            .get_or_try_init(|| build_channel(&self.endpoint))
            .await?;
        Ok(channel.clone())
    }

    /// Commits one batch, reconnecting and resending across
    /// transient failures with backoff.
    pub(crate) async fn write_batch(&self, writes: Vec<Write>) -> Result<CommittedBatch> {
        let mut backoff = Backoff::new();
        let mut force_refresh = false;
        loop {
            match self.try_write_once(writes.clone(), force_refresh).await {
                Ok(ack) => return Ok(ack),
                Err(AttemptFailure::Batch(error)) => {
                    if is_permanent_batch_error(&error) {
                        self.abandon_outstanding();
                        return Err(error);
                    }
                    if matches!(error, Error::Unauthenticated(_)) {
                        if force_refresh {
                            self.abandon_outstanding();
                            return Err(error);
                        }
                        // Possibly a stale token: force one refresh
                        // and retry before treating it as permanent.
                        force_refresh = true;
                        continue;
                    }
                    tokio::time::sleep(backoff.next_delay()).await;
                }
                Err(AttemptFailure::Handshake(error)) => {
                    // No batch was sent on this stream, so the batch
                    // itself cannot have been condemned: failures
                    // while establishing the stream (including the
                    // database-name mismatch the frontend
                    // intermittently returns on a fresh stream) are
                    // retried with backoff, like the official clients
                    // retry stream establishment. Authentication is
                    // the exception: one forced refresh, then the
                    // failure is real.
                    if matches!(error, Error::Unauthenticated(_)) {
                        if force_refresh {
                            self.abandon_outstanding();
                            return Err(error);
                        }
                        force_refresh = true;
                        continue;
                    }
                    tokio::time::sleep(backoff.next_delay()).await;
                }
            }
        }
    }

    /// Clears the protocol's outstanding batch after the caller
    /// gives up on it (a permanent error): the batch will never be
    /// resent, so a later resume must not expect its replay.
    fn abandon_outstanding(&self) {
        if let Some(state) = self
            .salvaged
            .lock()
            .expect("salvage mutex poisoned")
            .as_mut()
        {
            state.protocol.abandon_pending();
            state.replayed_ack = None;
        }
    }

    /// Returns salvaged state to the shelf (a dead session's
    /// protocol together with any unconsumed replayed ack).
    fn restore_salvage(&self, protocol: WriteProtocol, replayed_ack: Option<CommittedBatch>) {
        *self.salvaged.lock().expect("salvage mutex poisoned") = Some(SalvagedState {
            protocol,
            replayed_ack,
        });
    }

    /// One attempt: ensure a handshaken session, send the batch,
    /// await its ack. Any stream failure salvages the protocol
    /// (with the batch still pending) for the next attempt.
    async fn try_write_once(
        &self,
        writes: Vec<Write>,
        force_refresh: bool,
    ) -> std::result::Result<CommittedBatch, AttemptFailure> {
        let mut guard = self.session.lock().await;
        if guard.is_none() {
            *guard = Some(
                self.connect(force_refresh)
                    .await
                    .map_err(AttemptFailure::Handshake)?,
            );
        }
        let session = guard.as_mut().expect("session just ensured");
        if let Some(ack) = session.replayed_ack.take() {
            // The resume replay already committed this batch.
            return Ok(ack);
        }
        let request = session.protocol.batch_request(writes);
        if session.sender.send(request).await.is_err() {
            let mut session = guard.take().expect("session present");
            session.protocol.on_stream_end();
            self.restore_salvage(session.protocol, None);
            return Err(AttemptFailure::Batch(Error::Other(
                "write stream request channel closed".to_string(),
            )));
        }
        match session.responses.message().await {
            Ok(Some(response)) => match session.protocol.on_response(response) {
                Ok(WriteOutcome::BatchAck(ack)) => Ok(ack),
                Ok(WriteOutcome::Handshake) => Err(AttemptFailure::Batch(Error::Other(
                    "unexpected extra handshake response on write stream".to_string(),
                ))),
                Ok(WriteOutcome::ReplayedAck(_)) => Err(AttemptFailure::Batch(Error::Other(
                    "unexpected replayed acknowledgement on an established stream".to_string(),
                ))),
                Err(error) => Err(AttemptFailure::Batch(error)),
            },
            Ok(None) => {
                let mut session = guard.take().expect("session present");
                session.protocol.on_stream_end();
                self.restore_salvage(session.protocol, None);
                Err(AttemptFailure::Batch(Error::Other(
                    "write stream closed by the server".to_string(),
                )))
            }
            Err(status) => {
                let mut session = guard.take().expect("session present");
                session.protocol.on_stream_end();
                self.restore_salvage(session.protocol, None);
                Err(AttemptFailure::Batch(map_status(status)))
            }
        }
    }

    /// Opens a stream and completes the handshake (first response
    /// awaited before returning).
    async fn connect(&self, force_refresh: bool) -> Result<Session> {
        let channel = self.channel().await?;
        let mut client: FirestoreClient<_> =
            grpc_client(channel, &self.options, &self.tokens, force_refresh).await?;
        let (sender, receiver) = tokio::sync::mpsc::channel(8);
        // Take the salvaged state (or start fresh). A replayed ack
        // restored from a previous, half-completed resume counts:
        // the server will not replay it again (the token already
        // advanced past that commit), so it must reach the caller
        // through this session.
        let SalvagedState {
            mut protocol,
            mut replayed_ack,
        } = self
            .salvaged
            .lock()
            .expect("salvage mutex poisoned")
            .take()
            .unwrap_or_else(|| SalvagedState {
                protocol: WriteProtocol::new(self.options.database_name()),
                replayed_ack: None,
            });
        let handshake = protocol.handshake_request();
        if sender.send(handshake).await.is_err() {
            self.restore_salvage(protocol, replayed_ack);
            return Err(Error::Other(
                "write stream request channel closed".to_string(),
            ));
        }
        // As with Listen, the streaming RPC needs the database
        // resource prefix header beside the routing params.
        let mut request = Request::new(tokio_stream::wrappers::ReceiverStream::new(receiver));
        if let Ok(prefix) = self.options.database_name().parse() {
            request
                .metadata_mut()
                .insert("google-cloud-resource-prefix", prefix);
        }
        let mut responses = match client.write(request).await {
            Ok(response) => response.into_inner(),
            Err(status) => {
                self.restore_salvage(protocol, replayed_ack);
                return Err(map_status(status));
            }
        };
        // Read until the handshake completes. On a resumed stream
        // the server first replays any commit responses past the
        // resume token (at most one batch can be outstanding in
        // this single-flight driver), then sends the token-only
        // handshake response. Every failure below restores BOTH the
        // protocol and any replayed ack consumed so far: losing
        // the ack would resend a batch that already committed.
        loop {
            match responses.message().await {
                Ok(Some(response)) => match protocol.on_response(response) {
                    Ok(WriteOutcome::Handshake) => {
                        return Ok(Session {
                            protocol,
                            sender,
                            responses,
                            replayed_ack,
                        });
                    }
                    Ok(WriteOutcome::ReplayedAck(ack)) => {
                        if replayed_ack.is_some() {
                            self.restore_salvage(protocol, replayed_ack);
                            return Err(Error::Other(
                                "server replayed more than one batch on resume".to_string(),
                            ));
                        }
                        replayed_ack = Some(ack);
                    }
                    Ok(WriteOutcome::BatchAck(_)) => {
                        self.restore_salvage(protocol, replayed_ack);
                        return Err(Error::Other(
                            "write stream acknowledged a batch during handshake".to_string(),
                        ));
                    }
                    Err(error) => {
                        self.restore_salvage(protocol, replayed_ack);
                        return Err(error);
                    }
                },
                Ok(None) => {
                    self.restore_salvage(protocol, replayed_ack);
                    return Err(Error::Other(
                        "write stream closed during handshake".to_string(),
                    ));
                }
                Err(status) => {
                    self.restore_salvage(protocol, replayed_ack);
                    return Err(map_status(status));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::google::firestore::v1::WriteResult;

    fn protocol_after_handshake() -> WriteProtocol {
        let mut protocol = WriteProtocol::new("projects/p/databases/d".to_string());
        let outcome = protocol
            .on_response(WriteResponse {
                stream_id: "stream-1".to_string(),
                stream_token: vec![9],
                write_results: Vec::new(),
                commit_time: None,
            })
            .unwrap();
        assert_eq!(outcome, WriteOutcome::Handshake);
        protocol
    }

    #[test]
    fn handshake_precedes_batches() {
        let protocol = WriteProtocol::new("projects/p/databases/d".to_string());
        let request = protocol.handshake_request();
        assert_eq!(request.database, "projects/p/databases/d");
        assert!(request.writes.is_empty());
        let protocol = protocol_after_handshake();
        assert!(!protocol.has_pending());
    }

    #[test]
    fn batch_ack_maps_results_and_token() {
        let mut protocol = protocol_after_handshake();
        let request = protocol.batch_request(vec![Write::default(), Write::default()]);
        assert_eq!(request.stream_token, vec![9]);
        assert_eq!(request.writes.len(), 2);
        let outcome = protocol
            .on_response(WriteResponse {
                stream_id: String::new(),
                stream_token: vec![10],
                write_results: vec![
                    WriteResult {
                        update_time: Some(prost_types::Timestamp {
                            seconds: 5,
                            nanos: 0,
                        }),
                        transform_results: Vec::new(),
                    },
                    WriteResult {
                        update_time: None,
                        transform_results: Vec::new(),
                    },
                ],
                commit_time: Some(prost_types::Timestamp {
                    seconds: 6,
                    nanos: 0,
                }),
            })
            .unwrap();
        match outcome {
            WriteOutcome::BatchAck(ack) => {
                assert_eq!(ack.commit_time, Some(Timestamp::new(6, 0)));
                assert_eq!(ack.update_times, vec![Some(Timestamp::new(5, 0)), None]);
            }
            other => panic!("unexpected: {other:?}"),
        }
        // The next batch carries the advanced token.
        let request = protocol.batch_request(vec![Write::default()]);
        assert_eq!(request.stream_token, vec![10]);
    }

    #[test]
    fn stream_end_keeps_resume_state_and_pending() {
        let mut protocol = protocol_after_handshake();
        let _ = protocol.batch_request(vec![Write::default()]);
        assert!(protocol.on_stream_end());
        // The outstanding batch survives the stream end (it is the
        // resume replay, not optimism, that decides its fate).
        assert!(protocol.has_pending());
        assert!(protocol.on_stream_end());
        // Resume: the handshake request carries the old stream id/token.
        let request = protocol.handshake_request();
        assert_eq!(request.stream_id, "stream-1");
        assert_eq!(request.stream_token, vec![9]);
    }

    fn commit_response(token: u8, results: usize) -> WriteResponse {
        WriteResponse {
            stream_id: String::new(),
            stream_token: vec![token],
            write_results: (0..results)
                .map(|_| WriteResult {
                    update_time: None,
                    transform_results: Vec::new(),
                })
                .collect(),
            commit_time: Some(prost_types::Timestamp {
                seconds: 42,
                nanos: 0,
            }),
        }
    }

    #[test]
    fn resume_replays_outstanding_commit_before_handshake() {
        let mut protocol = protocol_after_handshake();
        let _ = protocol.batch_request(vec![Write::default()]);
        assert!(protocol.on_stream_end());
        // Resumed stream: the commit response is replayed first...
        let outcome = protocol.on_response(commit_response(11, 1)).unwrap();
        match outcome {
            WriteOutcome::ReplayedAck(ack) => {
                assert_eq!(ack.commit_time, Some(Timestamp::new(42, 0)));
            }
            other => panic!("unexpected: {other:?}"),
        }
        assert!(!protocol.has_pending());
        // ...then the token-only handshake response completes the
        // resume, and a fresh batch may be sent (nothing resent).
        let outcome = protocol.on_response(commit_response(12, 0)).unwrap();
        assert_eq!(outcome, WriteOutcome::Handshake);
        let _ = protocol.batch_request(vec![Write::default(), Write::default()]);
        assert!(protocol.has_pending());
    }

    #[test]
    fn resume_without_replay_resends_after_handshake() {
        let mut protocol = protocol_after_handshake();
        let _ = protocol.batch_request(vec![Write::default()]);
        assert!(protocol.on_stream_end());
        // No replay: the token-only response arrives directly, the
        // batch is still outstanding, and resending it (same size)
        // is the batch_request path after the handshake.
        let outcome = protocol.on_response(commit_response(11, 0)).unwrap();
        assert_eq!(outcome, WriteOutcome::Handshake);
        assert!(protocol.has_pending());
        let request = protocol.batch_request(vec![Write::default()]);
        assert_eq!(request.writes.len(), 1);
    }

    #[test]
    fn permanent_error_classification() {
        assert!(is_permanent_batch_error(&Error::NotFound("x".to_string())));
        assert!(is_permanent_batch_error(&Error::Rpc {
            code: "FailedPrecondition".to_string(),
            message: "x".to_string(),
        }));
        assert!(!is_permanent_batch_error(&Error::Rpc {
            code: "Unavailable".to_string(),
            message: "x".to_string(),
        }));
    }
}
