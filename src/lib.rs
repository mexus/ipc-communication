//! IPC communication helper crate.

#![deny(missing_docs)]

use crossbeam_utils::thread::scope;
use ipc_channel::ipc::{channel, IpcReceiver, IpcSender};
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::{
    any::Any,
    collections::HashMap,
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{channel as inner_channel, Sender},
        Arc,
    },
};

pub mod labor;
mod panic_error;

pub use panic_error::PanicError;

#[derive(Clone, Debug)]
struct ThreadGuard {
    sender: Sender<()>,
}

impl Drop for ThreadGuard {
    fn drop(&mut self) {
        let _ = self.sender.send(());
    }
}

/// IPC communication error.
#[derive(Snafu, Debug)]
pub enum Error {
    /// Unable to initialize channels.
    #[snafu(display(
        "Unable to initialize {} channels (at channel #{}): {}",
        channels,
        channel_id,
        source,
    ))]
    ChannelsInit {
        /// Source I/O error.
        source: io::Error,

        /// Channel ID.
        channel_id: usize,

        /// Channels amount.
        channels: usize,
    },

    /// Channel not found while making a request.
    #[snafu(display(
        "Can't make request, since there is no channel #{} ({} total channels)",
        channel_id,
        channels
    ))]
    ChannelNotFound {
        /// Channel ID.
        channel_id: usize,

        /// Channels amount.
        channels: usize,
    },

    /// Unable to initialize a channel for a response.
    #[snafu(display(
        "Unable to initialize a channel for a response while working on channel #{}: {}",
        channel_id,
        source
    ))]
    ResponseChannelInit {
        /// Source I/O error.
        source: io::Error,

        /// Channel ID.
        channel_id: usize,
    },

    /// Unable to initialize a quit confirmation channel.
    #[snafu(display("Unable to initialize a quit confirmation channel: {}", source))]
    QuitChannelInit {
        /// Source I/O error,
        source: io::Error,
    },

    /// Unable to send a request on a channel.
    #[snafu(display("Unable to send a request on a channel #{}: {}", channel_id, source))]
    SendingRequest {
        /// Source IPC error.
        source: ipc_channel::Error,

        /// Channel ID.
        channel_id: usize,
    },

    /// Unable to receiver a response on a channel.
    #[snafu(display(
        "Unable to receiver a response on a channel #{}: {}",
        channel_id,
        source
    ))]
    ReceivingResponse {
        /// Source IPC error.
        source: ipc_channel::Error,

        /// Channel ID.
        channel_id: usize,
    },

    /// Unable to receive a request on a channel.
    #[snafu(display("Unable to receive a request on a channel: {}", source))]
    ReceivingRequest {
        /// Source IPC error.
        source: ipc_channel::Error,
    },

    /// Unable to send a response on a channel.
    #[snafu(display("Unable to send a response on a channel: {}", source))]
    SendingResponse {
        /// Source IPC error.
        source: ipc_channel::Error,
    },

    /// Unable to send a request because a system has stopped.
    #[snafu(display("Unable to send a request because a system has stopped"))]
    StoppedSendingRequest,

    /// Unable to receive a response because a system has stopped.
    #[snafu(display("Unable to receive a response because a system has stopped"))]
    StoppedReceivingResponse,
}

impl Error {
    /// Checks if the other end has terminated.
    pub fn has_terminated(&self) -> bool {
        self.ipc_error()
            .map(|source| {
                if let ipc_channel::ErrorKind::Io(io) = source {
                    io.kind() == std::io::ErrorKind::BrokenPipe
                } else {
                    false
                }
            })
            .unwrap_or(false)
    }

    /// Checks if the Error happened because the system was stopped.
    pub fn has_stopped(&self) -> bool {
        match self {
            Error::StoppedSendingRequest | Error::StoppedReceivingResponse => true,
            _ => false,
        }
    }

    /// Returns the underlying `ipc-channel` error, if any.
    pub fn ipc_error(&self) -> Option<&ipc_channel::ErrorKind> {
        match self {
            Error::SendingRequest { source, .. }
            | Error::ReceivingResponse { source, .. }
            | Error::ReceivingRequest { source, .. }
            | Error::SendingResponse { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[derive(Serialize, Deserialize)]
enum Message<Request, Response> {
    Request {
        request: Request,
        respond_to: u64,
    },
    Register {
        client_id: u64,
        sender: IpcSender<Response>,
    },
    Unregister {
        client_id: u64,
    },
    Quit,
}

/// An immutable clients builder.
pub struct ClientBuilder<Request, Response>
where
    Request: Serialize,
    Response: Serialize,
{
    senders: Vec<IpcSender<Message<Request, Response>>>,
    running: Arc<AtomicBool>,
}

impl<Request, Response> Clone for ClientBuilder<Request, Response>
where
    for<'de> Request: Deserialize<'de> + Serialize,
    for<'de> Response: Deserialize<'de> + Serialize,
{
    fn clone(&self) -> Self {
        Self {
            senders: self.senders.clone(),
            running: Arc::clone(&self.running),
        }
    }
}

impl<Request, Response> ClientBuilder<Request, Response>
where
    for<'de> Request: Deserialize<'de> + Serialize,
    for<'de> Response: Deserialize<'de> + Serialize,
{
    /// Builds a client.
    pub fn build(&self) -> Client<Request, Response> {
        Client::new(self.senders.clone(), &self.running)
    }
}

/// A "client" capable of sending requests to processors.
pub struct Client<Request, Response>
where
    Request: Serialize,
    Response: Serialize,
{
    id: u64,
    senders: Vec<IpcSender<Message<Request, Response>>>,
    receiver: IpcReceiver<Response>,
    running: Arc<AtomicBool>,
}

impl<Request, Response> Drop for Client<Request, Response>
where
    Request: Serialize,
    Response: Serialize,
{
    fn drop(&mut self) {
        for server_sender in &self.senders {
            let _ = server_sender.send(Message::Unregister { client_id: self.id });
        }
    }
}

impl<Request, Response> Clone for Client<Request, Response>
where
    for<'de> Request: Deserialize<'de> + Serialize,
    for<'de> Response: Deserialize<'de> + Serialize,
{
    fn clone(&self) -> Self {
        Client::new(self.senders.clone(), &self.running)
    }
}

impl<Request, Response> Client<Request, Response>
where
    for<'de> Request: Deserialize<'de> + Serialize,
    for<'de> Response: Deserialize<'de> + Serialize,
{
    fn new(senders: Vec<IpcSender<Message<Request, Response>>>, running: &Arc<AtomicBool>) -> Self {
        let new_id = rand::Rng::gen(&mut rand::thread_rng());
        let (sender, receiver) =
            channel().expect("Can't initialize a sender-receiver pair; shouldn't fail");
        for server_sender in &senders {
            server_sender
                .send(Message::Register {
                    client_id: new_id,
                    sender: sender.clone(),
                })
                .expect("Unable to register a client");
        }
        Client {
            id: new_id,
            senders,
            running: Arc::clone(running),
            receiver,
        }
    }

    /// Sends a request to a given channel id and waits for a response.
    #[allow(clippy::redundant_clone)]
    pub fn make_request(&self, channel_id: usize, request: Request) -> Result<Response, Error> {
        let channels = self.senders.len();
        let sender = self.senders.get(channel_id).context(ChannelNotFound {
            channel_id,
            channels,
        })?;
        ensure!(self.running.load(Ordering::SeqCst), StoppedSendingRequest);
        sender
            .send(Message::Request {
                request,
                respond_to: self.id,
            })
            .context(SendingRequest { channel_id })?;
        ensure!(
            self.running.load(Ordering::SeqCst),
            StoppedReceivingResponse
        );
        self.receiver
            .recv()
            .context(ReceivingResponse { channel_id })
        // rcv.recv().context(ReceivingResponse { channel_id })
    }

    /// Returns the amount of channels.
    pub fn channels(&self) -> usize {
        self.senders.len()
    }
}

/// Requests processor.
pub struct Processor<Request, Response> {
    receiver: IpcReceiver<Message<Request, Response>>,
}

/// A result returned by a "loafer".
#[derive(Debug, Clone, Copy)]
pub enum LoaferResult {
    /// The caller can block on receiving data, since the loafer has done all it needed.
    ImDone,

    /// A hint to call the loafer again.
    CallMeAgain,
}

fn is_would_block(e: &ipc_channel::Error) -> bool {
    if let ipc_channel::ErrorKind::Io(io) = &e as &_ {
        io.kind() == std::io::ErrorKind::WouldBlock
    } else {
        false
    }
}

fn maybe_message<T>(rcv: &IpcReceiver<T>) -> Result<Option<T>, ipc_channel::Error>
where
    for<'de> T: Deserialize<'de> + Serialize,
{
    match rcv.try_recv() {
        Ok(item) => Ok(Some(item)),
        Err(e) if is_would_block(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

impl<Request, Response> Processor<Request, Response>
where
    for<'de> Request: Serialize + Deserialize<'de>,
    for<'de> Response: Serialize + Deserialize<'de>,
{
    /// Runs infinitely, processing incoming request using a given closure and sending generated
    /// responses back to the clients.
    ///
    /// The `loafer` is called every time there are no more messages in the queue.
    pub fn run_loop<P>(&self, mut proletarian: P) -> Result<(), Error>
    where
        P: labor::Proletarian<Request, Response>,
    {
        let mut should_block = false;
        let mut clients = HashMap::<u64, IpcSender<Response>>::new();
        loop {
            let item = if should_block {
                self.receiver.recv().context(ReceivingRequest)?
            } else if let Some(item) = maybe_message(&self.receiver).context(ReceivingRequest)? {
                item
            } else {
                match proletarian.loaf() {
                    labor::LoafingResult::ImDone => {
                        should_block = true;
                        continue;
                    }
                    labor::LoafingResult::TouchMeAgain => {
                        should_block = false;
                        continue;
                    }
                }
            };
            should_block = false;
            match item {
                Message::Quit => break Ok(()),
                Message::Request {
                    request,
                    respond_to,
                } => {
                    if let Some(response_channel) = clients.get(&respond_to) {
                        let response = proletarian.process_request(request);
                        if let Err(e) = response_channel.send(response).context(SendingResponse) {
                            // Do not stop execution when sending a response fails.
                            log::error!("Unable to send a response: {}", e);
                        }
                    } else {
                        log::error!("Unknown client with id {}", respond_to);
                    }
                }
                Message::Register { client_id, sender } => {
                    if clients.insert(client_id, sender).is_some() {
                        log::warn!("Client {} was already registered..", client_id);
                    }
                }
                Message::Unregister { client_id } => {
                    if clients.remove(&client_id).is_none() {
                        log::warn!("Client {} is not registered..", client_id);
                    }
                }
            }
        }
    }
}

/// Request processors.
#[must_use = "One must call process requests in order for the communication to run"]
pub struct Processors<Request, Response> {
    /// The underlying processors.
    pub processors: Vec<Processor<Request, Response>>,

    /// Processors handle.
    handle: ProcessorsHandle<Request, Response>,
}

/// Processors handler.
pub struct ProcessorsHandle<Request, Response> {
    senders: Vec<IpcSender<Message<Request, Response>>>,
    running: Arc<AtomicBool>,
}

impl<Request, Response> Clone for ProcessorsHandle<Request, Response>
where
    for<'de> Request: Deserialize<'de> + Serialize,
    for<'de> Response: Deserialize<'de> + Serialize,
{
    fn clone(&self) -> Self {
        ProcessorsHandle {
            senders: self.senders.clone(),
            running: self.running.clone(),
        }
    }
}

impl<Request, Response> ProcessorsHandle<Request, Response>
where
    for<'de> Request: Deserialize<'de> + Serialize,
    for<'de> Response: Deserialize<'de> + Serialize,
{
    /// Sends a stop signal to all the running processors and waits for them to receive the signal.
    pub fn stop(&self) -> Result<(), Error> {
        self.running.store(false, Ordering::SeqCst);
        self.senders.iter().for_each(|sender| {
            let _ = sender.send(Message::Quit);
        });
        Ok(())
    }
}

/// An error that happened during a parallel execution of processors.
#[derive(Snafu, Debug)]
pub enum ParallelRunError {
    /// A thread has panicked.
    #[snafu(display("Thread {:?} panicked: {}", thread_name, source))]
    ThreadPanic {
        /// Thread name.
        thread_name: String,

        /// Panic message.
        #[snafu(source(from(Box<dyn Any + Send + 'static>, PanicError::new)))]
        source: PanicError,
    },

    /// An unknown thread has panicked. Shouldn't happen.
    #[snafu(display("Non-joined thread panicked: {}", source))]
    UnjoinedThreadPanic {
        /// Panic message.
        #[snafu(source(from(Box<dyn Any + Send + 'static>, PanicError::new)))]
        source: PanicError,
    },

    /// IPC communication error.
    #[snafu(display("Thread {:?} terminated with error: {}", thread_name, source))]
    IpcError {
        /// Thread name.
        thread_name: String,

        /// IPC error.
        source: Error,
    },

    /// Spawn of a processor failed.
    #[snafu(display(
        "Failed to spawn a thread for processing channel #{}: {}",
        channel_id,
        source
    ))]
    SpawnError {
        /// Channel ID.
        channel_id: usize,

        /// Spawn I/O error.
        source: io::Error,
    },
}

impl<Request, Response> Processors<Request, Response>
where
    for<'de> Request: Serialize + Deserialize<'de> + Send,
    for<'de> Response: Serialize + Deserialize<'de> + Send,
{
    /// Runs all the underlying responses in separate thread each using given socium.
    pub fn run_in_parallel<S>(self, socium: S) -> Result<Vec<ParallelRunError>, ParallelRunError>
    where
        S: labor::Socium<Request, Response> + Sync,
        S::Proletarian: labor::Proletarian<Request, Response>,
    {
        let res = scope(|s| {
            let (tx, rx) = inner_channel::<()>();
            let handlers = self
                .processors
                .into_iter()
                .enumerate()
                .map(|(channel_id, processor)| {
                    let name = format!("Channel #{}", channel_id);
                    let socium = &socium;
                    let tx = tx.clone();
                    s.builder()
                        .name(name)
                        .spawn(move |_| {
                            let _guard = ThreadGuard { sender: tx };
                            let prolet = socium.construct_proletarian(channel_id);
                            processor.run_loop(prolet)
                        })
                        .context(SpawnError { channel_id })
                })
                .collect::<Result<Vec<_>, _>>()?;

            // Wait for the first channel to end and then join 'em all!
            let _ = rx.recv();
            let _ = self.handle.stop();

            let join_errors: Vec<_> = handlers
                .into_iter()
                .map(|handler| {
                    let thread_name = handler
                        .thread()
                        .name()
                        .unwrap_or("[unknown thread]")
                        .to_string();
                    let thread_name = &thread_name;
                    handler
                        .join()
                        .context(ThreadPanic { thread_name })?
                        .context(IpcError { thread_name })
                })
                .filter_map(|res| match res {
                    Ok(()) => None,
                    Err(e) => Some(e),
                })
                .collect();
            Ok(join_errors)
        })
        .context(UnjoinedThreadPanic)??;
        Ok(res)
    }
}

/// A helper structure that contains communication objects.
pub struct Communication<Request, Response>
where
    Request: Serialize,
    Response: Serialize,
{
    /// A clonable client builder.
    pub client_builder: ClientBuilder<Request, Response>,

    /// A processors container.
    pub processors: Processors<Request, Response>,

    /// A processors control handle.
    pub handle: ProcessorsHandle<Request, Response>,
}

/// Sets up communication channels for a given request-response pairs.
///
/// `Client` is clonable and can be sent across multiple threads or even processes.
///
/// `Processor`s, on the other hand, are not clonable and can not be accessed from
/// multiple threads simultaneously, but still can be sent across threads and processes.
pub fn communication<Request, Response>(
    channels: usize,
) -> Result<Communication<Request, Response>, Error>
where
    for<'de> Request: Deserialize<'de> + Serialize,
    for<'de> Response: Deserialize<'de> + Serialize,
{
    let mut processors = Vec::with_capacity(channels);
    let mut senders = Vec::with_capacity(channels);
    for channel_id in 0..channels {
        let (sender, receiver) = channel().context(ChannelsInit {
            channel_id,
            channels,
        })?;
        processors.push(Processor { receiver });
        senders.push(sender);
    }
    let running = Arc::new(AtomicBool::new(true));
    let handle = ProcessorsHandle {
        senders: senders.clone(),
        running: Arc::clone(&running),
    };
    let client_builder = ClientBuilder { senders, running };
    let processors = Processors {
        processors,
        handle: handle.clone(),
    };
    Ok(Communication {
        client_builder,
        processors,
        handle,
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::{distributions::Standard, prelude::*};

    #[test]
    fn check() {
        const CHANNELS: usize = 4;
        const MAX_LEN: usize = 1024;
        const CLIENT_THREADS: usize = 100;
        const MESSAGES_PER_CLIENT: usize = 100;

        let Communication {
            client_builder,
            processors,
            handle,
        } = communication::<Vec<u8>, usize>(CHANNELS).unwrap();

        let processors = std::thread::spawn(move || {
            processors
                .run_in_parallel(|_channel_id| |v: Vec<_>| v.len())
                .unwrap()
        });
        scope(|s| {
            for _ in 0..CLIENT_THREADS {
                let client_builder = client_builder.clone();
                s.spawn(move |_| {
                    let mut rng = thread_rng();
                    for _ in 0..MESSAGES_PER_CLIENT {
                        let channel_id = rng.gen_range(0, CHANNELS);
                        let length = rng.gen_range(0, MAX_LEN);
                        let data = rng.sample_iter(Standard).take(length).collect();

                        let client = client_builder.build();
                        let response = client.make_request(channel_id, data).unwrap();
                        assert_eq!(response, length);
                    }
                });
            }
        })
        .unwrap();
        handle.stop().unwrap();
        processors.join().unwrap();
    }
}
