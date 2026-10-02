//! Client side of the socket protocol, shared by the CLI and the tests.

use std::io::{self, BufReader, BufWriter};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

use crate::proto::{ClientFrame, PROTOCOL_VERSION, Request, Response, ServerFrame, read_frame, write_frame};

pub struct Client {
    reader: BufReader<UnixStream>,
    writer: BufWriter<UnixStream>,
}

#[derive(Debug)]
pub enum ClientError {
    /// The daemon is not reachable (not running, or the socket is missing).
    Unavailable(io::Error),
    Io(io::Error),
    /// The daemon closed the connection without a response.
    Closed,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Unavailable(e) => write!(f, "chatd is not reachable ({e}); check: systemctl --user status chatd"),
            ClientError::Io(e) => write!(f, "connection to chatd failed: {e}"),
            ClientError::Closed => write!(f, "chatd closed the connection without a response"),
        }
    }
}

impl Client {
    /// Connects (a local socket connect either succeeds or fails at once). `io_timeout` bounds
    /// each read/write; `None` for watches, which rely on heartbeats.
    pub fn connect(socket: &Path, io_timeout: Option<Duration>) -> Result<Client, ClientError> {
        let stream = UnixStream::connect(socket).map_err(ClientError::Unavailable)?;
        stream.set_read_timeout(io_timeout).map_err(ClientError::Io)?;
        stream.set_write_timeout(Some(io_timeout.unwrap_or(Duration::from_secs(30)))).map_err(ClientError::Io)?;
        let reader = BufReader::new(stream.try_clone().map_err(ClientError::Io)?);
        Ok(Client { reader, writer: BufWriter::new(stream) })
    }

    pub fn set_read_timeout(&self, t: Option<Duration>) -> io::Result<()> {
        self.reader.get_ref().set_read_timeout(t)
    }

    pub fn send(&mut self, req: Request) -> Result<(), ClientError> {
        write_frame(&mut self.writer, &ClientFrame { v: PROTOCOL_VERSION, req }).map_err(ClientError::Io)
    }

    pub fn recv(&mut self) -> Result<Response, ClientError> {
        match read_frame::<_, ServerFrame>(&mut self.reader) {
            Ok(Some(f)) => Ok(f.resp),
            Ok(None) => Err(ClientError::Closed),
            Err(e) => Err(ClientError::Io(e)),
        }
    }

    pub fn call(&mut self, req: Request) -> Result<Response, ClientError> {
        self.send(req)?;
        self.recv()
    }
}
