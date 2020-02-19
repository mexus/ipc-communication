//! IPC communication helper crate.

#![deny(missing_docs)]

use crossbeam_utils::thread::scope;
use ipc_channel::ipc::{channel, IpcReceiver, IpcSender};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use std::{any::Any, convert::identity, io};

/// Non-constructible helper data type, just like `!`, but works on stable.
pub enum Never {}

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

    /// Unable to initialize a quit confirmation channel
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
        let (snd, rcv) = channel::<Response>().context(ResponseChannelInit { channel_id })?;
        sender
            .send(Message::Request {
                request,
                // We clone the `snd` in order to make sure we won't get
                // "All senders for this socket closed" error.
                response_channel: snd.clone(),
            })
            .context(SendingRequest { channel_id })?;
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

impl<Request, Response> Processor<Request, Response>
where
    Request: Serialize,
    for<'de> Request: Deserialize<'de>,
    Response: Serialize,
    for<'de> Response: Deserialize<'de>,
{
    /// Runs infinitely, processing incoming request using a given closure and sending generated
    /// responses back to the clients.
    pub fn run_loop<F>(&self, mut f: F) -> Result<(), Error>
    where
        F: FnMut(Request) -> Response,
    {
        loop {
            match self.receiver.recv().context(ReceivingRequest)? {
                Message::Quit { response_channel } => {
                    response_channel.send(()).context(SendingQuitResponse)?;
                    break Ok(());
                }
                Message::Request {
                    request,
                    response_channel,
                } => {
                    let response = f(request);
                    response_channel.send(response).context(SendingResponse)?;
                }
            }
        }
    }
}

/// Request processors.
#[must_use = "One must call process requests in order for the communication to run"]
pub struct Processors<Request, Response>(
    /// The underlying processors.
    pub Vec<Processor<Request, Response>>,
);

/// Processors handler.
pub struct ProcessorsHandle<Request, Response> {
    senders: Vec<IpcSender<Message<Request, Response>>>,
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
    Request: Serialize,
    for<'de> Request: Deserialize<'de>,
    Response: Serialize,
    for<'de> Response: Deserialize<'de>,
    Request: Send,
    Response: Send,
{
    /// Runs all the underlying responses in separate thread each using a given closure.
    pub fn run_in_parallel<S, State, F>(
        self,
        mut setup: S,
        f: F,
    ) -> Result<(), Vec<ParallelRunError>>
    where
        S: FnMut() -> State,
        F: Fn(&mut State, Request) -> Response,
        F: Send + Sync,
        State: Send,
    {
        let res = scope(|s| {
            let handlers = self
                .0
                .into_iter()
                .enumerate()
                .map(|(channel_id, processor)| {
                    let f = &f;
                    let name = format!("Processing channel #{}", channel_id);
                    let mut state = setup();
                    s.builder()
                        .name(name)
                        .spawn(move |_| processor.run_loop(|request| f(&mut state, request)))
                        .context(SpawnError { channel_id })
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| vec![e])?;
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
            if join_errors.is_empty() {
                Ok(())
            } else {
                Err(join_errors)
            }
        })
        .context(UnjoinedThreadPanic)
        .map_err(|e| vec![e]);
        res.and_then(identity)
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
    let handle = ProcessorsHandle {
        senders: senders.clone(),
    };
    let client = Client { senders };
    let processors = Processors(processors);
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

        let processors =
            std::thread::spawn(move || processors.run_in_parallel(|| (), |_, v| v.len()).unwrap());
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
