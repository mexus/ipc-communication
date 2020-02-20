//! Labor traits.

/// A result returned by a "loafer".
#[derive(Debug, Clone, Copy)]
pub enum LoafingResult {
    /// The caller can block on receiving data, since the loafer has done all it needed.
    ImDone,

    /// A hint to call the loafer again.
    TouchMeAgain,
}

/// The one that does the hard job.
pub trait Proletarian<Request, Response> {
    /// Processes a request.
    fn process_request(&mut self, request: Request) -> Response;

    /// Loafs a bit, e.g. when there are no incoming requests.
    fn loaf(&mut self) -> LoafingResult {
        LoafingResult::ImDone
    }
}

/// A `Proletarian` fabric.
pub trait Socium<Request, Response> {
    /// The one that does the hard job.
    type Proletarian: Proletarian<Request, Response>;

    /// Constructs an instance of a `Proletarian`.
    fn construct_proletarian(&mut self, channel_id: usize) -> Self::Proletarian;
}

impl<F: FnMut(Req) -> Resp, Req, Resp> Proletarian<Req, Resp> for F {
    fn process_request(&mut self, request: Req) -> Resp {
        (self)(request)
    }
}

impl<F, P, Req, Resp> Socium<Req, Resp> for F
where
    F: FnMut(usize) -> P,
    P: Proletarian<Req, Resp>,
{
    type Proletarian = P;

    fn construct_proletarian(&mut self, channel_id: usize) -> Self::Proletarian {
        (self)(channel_id)
    }
}
