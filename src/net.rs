use futures::io::{AsyncRead, AsyncWrite};
use std::io::{self, Read, Result, Write};
use std::net::{self, Shutdown, ToSocketAddrs};
use std::os::unix::io::AsRawFd;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::reactor::{self, Interest};

/// A hack-ish async wrapper over a `std::net::TcpStream`,
/// using its blocking `connect` and DNS resolution. After connection
/// the socket is marked non-blocking, for async reads and writes.
#[derive(Debug)]
pub struct TcpStream {
    inner: net::TcpStream,
}

impl TcpStream {
    /// Note: this is a blocking connect call "disguised" as an async one for simplicity reasons
    /// and usage compatibility higher-up in the stack.
    pub async fn connect(addr: impl ToSocketAddrs) -> Result<TcpStream>{
        Self::blocking_connect(addr)
    }

    pub fn blocking_connect(addr: impl ToSocketAddrs) -> Result<TcpStream> {
        let inner = net::TcpStream::connect(addr)?;
        inner.set_nonblocking(true)?;
        Ok(TcpStream { inner })
    }

    pub fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.inner.shutdown(how)
    }
}

impl AsRawFd for TcpStream {
    fn as_raw_fd(&self) -> i32 {
        self.inner.as_raw_fd()
    }
}

impl Drop for TcpStream {
    fn drop(&mut self) {
        let fd = self.as_raw_fd();
        reactor::unregister_interests(fd);
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        let fd = self.as_raw_fd();
        let waker = cx.waker();

        // println!("poll_read() called for fd {}", fd);

        match self.inner.read(buf) {
            Ok(len) => Poll::Ready(Ok(len)),
            Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => {
                // println!("TcpStream::read() on fd {} would block here", fd);
                reactor::register_interest(fd, Interest::Readable, waker.clone());
                Poll::Pending
            }
            Err(err) => panic!("error {:?}", err),
        }
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        let fd = self.as_raw_fd();
        let waker = cx.waker();

        // println!("poll_write() called for fd {}", fd);

        match self.inner.write(buf) {
            Ok(len) => Poll::Ready(Ok(len)),
            Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => {
                // println!("TcpStream::write() on fd {} would block here", fd);
                reactor::register_interest(fd, Interest::Writable, waker.clone());
                Poll::Pending
            }
            Err(err) => panic!("error {:?}", err),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        eprintln!("poll_flush() called");
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        eprintln!("poll_close() called");
        Poll::Ready(Ok(()))
    }
}
