use std::cell::Cell;
use std::future::Future;
use std::panic::catch_unwind;
use std::thread;

use crossbeam::atomic::AtomicCell;
use crossbeam::channel;
use once_cell::sync::Lazy;
use pyo3::prelude::*;

use crate::reactor;

#[derive(Clone, Copy, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct TaskId(usize);

/// A spawned future and its current state
type Task = async_task::Task<TaskId>;

/// A counter that assigns IDs to spawned tasks
static COUNTER: Lazy<AtomicCell<usize>> = Lazy::new(|| AtomicCell::new(0));

thread_local! {
    /// The ID of the current task.
    static TASK_ID: Cell<Option<TaskId>> = Cell::new(None);
}

/// Returns the ID of the currently executing task
///
/// Returns `None` if called outside the runtime
pub fn task_id() -> Option<TaskId> {
    TASK_ID.with(|id| id.get())
}

/// A queue that holds scheduled tasks
static QUEUE: Lazy<channel::Sender<Task>> = Lazy::new(|| {
    // Create a crossbeam queue
    let (sender, receiver) = channel::unbounded::<Task>();

    // Spawn executor threads the first time the queue is created
    let thread_count = num_cpus::get().max(1);
    for _ in 0..thread_count {
        let receiver = receiver.clone();
        thread::spawn(move || {
            TASK_ID.with(|id| {
                receiver.iter().for_each(|task| {
                    // Store the task ID into the thread-local before running
                    id.set(Some(*task.tag()));

                    // Ignore panics for simplicity
                    let _ = catch_unwind(|| task.run());
                })
            })
        });
    }

    sender
});

/// Spawns a future on the executor, bridged to python via:
/// - a completion callback
/// - two callbacks used to register IO interest in read and write events
///   for a given file descriptor
pub fn spawn_for_python<F, R>(
    future: F,
    on_done_callback: PyObject,
    read_registrar: PyObject,
    write_registrar: PyObject,
) where
    F: Future<Output = R> + Send + 'static,
    R: ToPyObject + Send + 'static,
{
    // Reserve an ID for the new task.
    let task_id = TaskId(COUNTER.fetch_add(1));

    // Record the IO registrars as task-local state in the reactor
    reactor::init_task_local_state(task_id, read_registrar, write_registrar);

    // Wrap the future into one that sends its output to python via the callback
    let future = async move {
        let ret = future.await;

        // Clean up the task-local state in the reactor
        reactor::cleanup_task_local_state(task_id);

        // Notify python that this future is complete, with the result
        // it returned
        crate::execute_python_callback(&on_done_callback, ret);
    };

    // Create a task and schedule it for execution
    let (task, _) = async_task::spawn(future, |t| QUEUE.send(t).unwrap(), task_id);
    task.schedule();
}

/// Spawns a future on the executor
pub fn spawn<F, R>(future: F)
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    // Reserve an ID for the new task.
    let task_id = TaskId(COUNTER.fetch_add(1));

    // Create a task and schedule it for execution
    let (task, _) = async_task::spawn(future, |t| QUEUE.send(t).unwrap(), task_id);
    task.schedule();
}
