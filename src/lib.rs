use pyo3::prelude::*;

pub mod executor;
pub mod net;
pub mod reactor;
pub mod timeout;
pub mod timer;

pub(crate) use executor::{task_id, TaskId};

pub use executor::spawn;
pub use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
pub use pyo3::PyObject;
pub use timeout::timeout;

use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

pub async fn sleep(dur: Duration) {
    let _: std::io::Result<()> = crate::timeout::io::timeout(dur, pending()).await;
}

async fn pending<T>() -> T {
    let fut = Pending {
        _marker: PhantomData,
    };
    fut.await
}

struct Pending<T> {
    _marker: PhantomData<T>,
}

impl<T> Future for Pending<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<T> {
        Poll::Pending
    }
}

pub fn execute_python_callback(callback: &PyObject, arg: impl ToPyObject) {
    let gil = Python::acquire_gil();
    let py = gil.python();

    let arg = arg.to_object(py);
    let args = (arg,);

    let res = callback.call1(py, args);
    match res {
        Ok(_) => {
            // the callback was successful, we're done
        }

        Err(e) => {
            // calling the callback failed, print the python error message,
            // and a traceback if available
            e.print_and_set_sys_last_vars(py);
        }
    }
}