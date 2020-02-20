//! IPC communication helper crate.

#![deny(missing_docs)]

use crossbeam_utils::thread::scope;
use ipc_channel::ipc::{channel, IpcReceiver, IpcSender};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::{
    any::Any,
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

pub mod labor;
mod panic_error;

pub use panic_error::PanicError;

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

    /// Unable to send a `quit` request.
    #[snafu(display("Unable to send a `quit` request: {}", source))]
    SendingQuitRequest {
        /// Source IPC error.
        source: ipc_channel::Error,
    },

    /// Unable to send a response to a `quit` command.
    #[snafu(display("Unable to send a response to a `quit` command: {}", source))]
    SendingQuitResponse {
        /// Source IPC error.
        source: ipc_channel::Error,
    },

    /// Unable to receive a response to a `quit` command.
    #[snafu(display("Unable to receive a response to a `quit` command: {}", source))]
    ReceivingQuitResponse {
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
            | Error::SendingResponse { source, .. }
            | Error::SendingQuitRequest { source, .. }
            | Error::SendingQuitResponse { source, .. }
            | Error::ReceivingQuitResponse { source, .. } => Some(source as &_),
            _ => None,
        }
    }
}

#[derive(Serialize, Deserialize)]
enum Message<Request, Response> {
    Request {
        request: Request,
        response_channel: IpcSender<Response>,
    },
    Quit {
        response_channel: IpcSender<()>,
    },
}

/// A "client" capable of sending requests to processors.
pub struct Client<Request, Response> {
    senders: Vec<IpcSender<Message<Request, Response>>>,
    running: Arc<AtomicBool>,
}

impl<Request, Response> Clone for Client<Request, Response>
where
    Request: Serialize,
    for<'de> Request: Deserialize<'de>,
    Response: Serialize,
    for<'de> Response: Deserialize<'de>,
{
    fn clone(&self) -> Self {
        Client {
            senders: self.senders.clone(),
            running: Arc::clone(&self.running),
        }
    }
}

impl<Request, Response> Client<Request, Response>
where
    Request: Serialize,
    for<'de> Request: Deserialize<'de>,
    Response: Serialize,
    for<'de> Response: Deserialize<'de>,
{
    /// Sends a request to a given channel id and waits for a response.
    pub fn make_request(&self, channel_id: usize, request: Request) -> Result<Response, Error> {
        let channels = self.senders.len();
        let sender = self.senders.get(channel_id).context(ChannelNotFound {
            channel_id,
            channels,
        })?;
        let (response_channel, rcv) =
            channel::<Response>().context(ResponseChannelInit { channel_id })?;
        ensure!(self.running.load(Ordering::SeqCst), StoppedSendingRequest);
        sender
            .send(Message::Request {
                request,
                response_channel,
            })
            .context(SendingRequest { channel_id })?;
        ensure!(
            self.running.load(Ordering::SeqCst),
            StoppedReceivingResponse
        );
        rcv.recv().context(ReceivingResponse { channel_id })
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
                Message::Quit { response_channel } => {
                    // Ignoring possible errors here.
                    let _ = response_channel.send(());
                    break Ok(());
                }
                Message::Request {
                    request,
                    response_channel,
                } => {
                    let response = proletarian.process_request(request);
                    if let Err(e) = response_channel.send(response).context(SendingResponse) {
                        // Do not stop execution when sending a response fails.
                        log::error!("Unable to send a response: {}", e);
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
}

/// Processors handler.
pub struct ProcessorsHandle<Request, Response> {
    senders: Vec<IpcSender<Message<Request, Response>>>,
    running: Arc<AtomicBool>,
}

impl<Request, Response> ProcessorsHandle<Request, Response>
where
    Request: Serialize,
    for<'de> Request: Deserialize<'de>,
    Response: Serialize,
    for<'de> Response: Deserialize<'de>,
{
    /// Sends a stop signal to all the running processors and waits for them to receive the signal.
    pub fn stop(self) -> Result<(), Vec<Error>> {
        self.running.store(false, Ordering::SeqCst);
        let (quit_confirmation, quit_rcv) = channel::<()>()
            .context(QuitChannelInit)
            .map_err(|e| vec![e])?;
        let (success_cnt, mut errors) = self
            .senders
            .into_iter()
            .map(|sender| -> Result<(), Error> {
                sender
                    .send(Message::Quit {
                        response_channel: quit_confirmation.clone(),
                    })
                    .context(SendingQuitRequest)?;
                Ok(())
            })
            .fold((0usize, Vec::new()), |(mut acc, mut errors), res| {
                match res {
                    Ok(()) => acc += 1,
                    Err(e) => errors.push(e),
                }
                (acc, errors)
            });
        let wait_errors =
            (0..success_cnt).filter_map(|_| match quit_rcv.recv().context(ReceivingQuitResponse) {
                Ok(()) => None,
                Err(e) => Some(e),
            });
        errors.extend(wait_errors);
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
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
    pub fn run_in_parallel<S>(
        self,
        mut socium: S,
    ) -> Result<Vec<ParallelRunError>, ParallelRunError>
    where
        S: labor::Socium<Request, Response>,
        S::Proletarian: labor::Proletarian<Request, Response> + Send,
    {
        let res = scope(|s| {
            let handlers = self
                .processors
                .into_iter()
                .enumerate()
                .map(|(channel_id, processor)| {
                    let name = format!("Processing channel #{}", channel_id);
                    let prolet = socium.construct_proletarian(channel_id);
                    s.builder()
                        .name(name)
                        .spawn(move |_| processor.run_loop(prolet))
                        .context(SpawnError { channel_id })
                })
                .collect::<Result<Vec<_>, _>>()?;
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

    /// Runs all the underlying responses in separate thread each using given socium.
    ///
    /// Like `run_in_parallel`, but can work with unsendable proletarians by wrapping the socium
    /// into a mutex and sending a reference to it accross the threads.
    pub fn run_in_parallel_unsend<S>(
        self,
        socium: S,
    ) -> Result<Vec<ParallelRunError>, ParallelRunError>
    where
        S: labor::Socium<Request, Response> + Send,
        S::Proletarian: labor::Proletarian<Request, Response>,
    {
        let socium = Mutex::new(socium);
        let res = scope(|s| {
            let handlers = self
                .processors
                .into_iter()
                .enumerate()
                .map(|(channel_id, processor)| {
                    let name = format!("Processing channel #{}", channel_id);
                    let socium = &socium;
                    s.builder()
                        .name(name)
                        .spawn(move |_| {
                            let prolet = socium.lock().construct_proletarian(channel_id);
                            processor.run_loop(prolet)
                        })
                        .context(SpawnError { channel_id })
                })
                .collect::<Result<Vec<_>, _>>()?;
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
pub struct Communication<Request, Response> {
    /// A clonable client.
    pub client: Client<Request, Response>,

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
    Request: Serialize,
    for<'de> Request: Deserialize<'de>,
    Response: Serialize,
    for<'de> Response: Deserialize<'de>,
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
    let client = Client {
        senders,
        running: Arc::clone(&running),
    };
    let processors = Processors { processors };
    Ok(Communication {
        client,
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
            client,
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
                let client = client.clone();
                s.spawn(move |_| {
                    let mut rng = thread_rng();
                    for _ in 0..MESSAGES_PER_CLIENT {
                        let channel_id = rng.gen_range(0, CHANNELS);
                        let length = rng.gen_range(0, MAX_LEN);
                        let data = rng.sample_iter(Standard).take(length).collect();

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
