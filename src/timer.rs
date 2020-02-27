use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;

/// Inefficient async timer example
pub struct AsyncTimer {
    state: Arc<Mutex<AsyncTimerState>>,
}

impl AsyncTimer {
    pub fn new(millis: u64) -> Self {
        let duration = Duration::from_millis(millis);

        let state = Arc::new(Mutex::new(AsyncTimerState {
            completed: false,
            waker: None,
        }));

        let thread_state = state.clone();
        thread::spawn(move || {
            thread::sleep(duration);

            let mut state = thread_state.lock().unwrap();
            state.completed = true;

            // Signal that the timer has completed and wake up the last
            // task on which the future was polled, if one exists.
            if let Some(waker) = state.waker.take() {
                println!("timer has completed, waking task up");
                waker.wake();
            }
        });

        AsyncTimer { state }
    }
}

struct AsyncTimerState {
    completed: bool,
    waker: Option<Waker>,
}

impl Future for AsyncTimer {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // println!("polling AsyncTimer future");

        let mut state = self.state.lock().unwrap();
        // println!("timer has completed: {}", state.completed);

        if state.completed {
            Poll::Ready(())
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
