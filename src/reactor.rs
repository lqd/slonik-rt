use std::collections::BTreeMap;
use std::os::unix::io::RawFd;
use std::task::Waker;
use std::thread;

use crossbeam::channel;
use once_cell::sync::Lazy;
use pyo3::prelude::*;

use crate::{task_id, TaskId};

/// An async rust reactor whose job is mostly to bridge
/// with the python asyncio stack when asked to register interest
/// in read/write availability on a file descriptor
#[derive(Default)]
struct Reactor {
    read: BTreeMap<RawFd, (TaskId, Waker)>,
    write: BTreeMap<RawFd, (TaskId, Waker)>,

    read_registrars: BTreeMap<TaskId, PyObject>,
    write_registrars: BTreeMap<TaskId, PyObject>,
}

static REACTOR: Lazy<channel::Sender<ReactorEvent>> = Lazy::new(|| {
    let (sender, receiver) = channel::unbounded::<ReactorEvent>();

    let mut reactor = Reactor::default();

    // Start the reactor thread, which will listen and handle the reactor events
    // sent to the channel
    thread::spawn(move || {
        receiver
            .iter()
            .for_each(|event| reactor.handle_event(event));
    });

    sender
});

impl Reactor {
    /// Handle interactions between the executor, `Future`s, and the OS (via python):
    /// - from the executor to the reactor: setup and tear down the task-local state
    ///   required to interact with the OS (via the python IO registrars)
    /// - from `Future`s to the OS: register and unregister interest in
    ///   read or write IO events for a file descriptor. They can then switch
    ///   to the `Pending` state, and wait to be polled again whenever the event occurs.
    /// - from the OS to the reactor: when python signals the availability of some
    ///   IO on a file descriptor, the task waiting on that event can be woken up
    fn handle_event(&mut self, event: ReactorEvent) {
        match event {
            ReactorEvent::RegisterInterest {
                task_id,
                fd,
                interest,
                waker,
            } => match interest {
                Interest::Readable => self.add_read_interest(task_id, fd, waker),
                Interest::Writable => self.add_write_interest(task_id, fd, waker),
            },

            ReactorEvent::UnregisterInterests { fd } => {
                // It's possible for the fd to be absent for the read/write queue, depending
                // on the kind of registered interest, and whether the IO event triggering
                // had already removed the task from the queue.
                self.read.remove(&fd);
                self.write.remove(&fd);
            }

            ReactorEvent::FdReady { fd, interest } => {
                let (_, waker) = match interest {
                    Interest::Readable => self.read.remove(&fd).expect("Read-waker wasn't found"),
                    Interest::Writable => self.write.remove(&fd).expect("Write-waker wasn't found"),
                };

                waker.wake_by_ref();
                // println!("task waiting on fd {} woken up", fd);
            }

            ReactorEvent::InstallRegistrars {
                task_id,
                read_registrar,
                write_registrar,
            } => {
                self.read_registrars.insert(task_id, read_registrar);
                self.write_registrars.insert(task_id, write_registrar);
            }

            ReactorEvent::UninstallRegistrars { task_id } => {
                self.read_registrars
                    .remove(&task_id)
                    .expect("Cleaning up task-local state failed");
                self.write_registrars
                    .remove(&task_id)
                    .expect("Cleaning up task-local state failed");
            }
        }
    }

    /// Futures will register interest in the file descriptor's
    /// "ready to be read" events
    fn add_read_interest(&mut self, task_id: TaskId, fd: RawFd, waker: Waker) {
        if !self.read.contains_key(&fd) {
            self.read.insert(fd, (task_id, waker));

            // println!(
            //     "rust task {:?} - notifying python of the read interest for fd {}",
            //     task_id, fd
            // );

            // Notify python of the IO interest for this fd
            let read_registrar = self
                .read_registrars
                .get(&task_id)
                .expect("Couldn't find read registrar in task-local state");
            crate::execute_python_callback(read_registrar, fd);
        } else {
            // There could be multiple IO interest on a fd for the same task, but arguably
            // not for different tasks
            let (registered_task, _) = self.read[&fd];
            if registered_task != task_id {
                panic!("a read waker for the task {:?} is already registered for this fd {}, {:?} would be a duplicate", registered_task, fd, task_id);
            }
        }
    }

    /// Futures will register interest in the file descriptor's
    /// "ready to be written" events
    fn add_write_interest(&mut self, task_id: TaskId, fd: RawFd, waker: Waker) {
        if !self.write.contains_key(&fd) {
            self.write.insert(fd, (task_id, waker));

            // println!(
            //     "rust task {:?} - notifying python of the write interest for fd {}",
            //     task_id, fd,
            // );

            // Notify python of the IO interest for this fd
            let write_registrar = self
                .write_registrars
                .get(&task_id)
                .expect("Couldn't find write registrar in task-local state");
            crate::execute_python_callback(write_registrar, fd);
        } else {
            // There could be multiple IO interest on a fd for the same task, but arguably
            // not for different tasks
            let (registered_task, _) = self.write[&fd];
            if registered_task != task_id {
                panic!("a write waker for the task {:?} is already registered for this fd {}, {:?} would be a duplicate", registered_task, fd, task_id);
            }
        }
    }
}

pub enum Interest {
    Readable,
    Writable,
}

/// The different kinds of events one can use to interact with the reactor.
enum ReactorEvent {
    RegisterInterest {
        task_id: TaskId,
        fd: RawFd,
        interest: Interest,
        waker: Waker,
    },

    FdReady {
        fd: RawFd,
        interest: Interest,
    },

    UnregisterInterests {
        fd: RawFd,
    },

    InstallRegistrars {
        task_id: TaskId,
        read_registrar: PyObject,
        write_registrar: PyObject,
    },

    UninstallRegistrars {
        task_id: TaskId,
    },
}

fn post_event(event: ReactorEvent) {
    REACTOR.send(event).unwrap();
}

pub(crate) fn register_interest(fd: RawFd, interest: Interest, waker: Waker) {
    let task_id = task_id().expect("Can't access current task id");
    post_event(ReactorEvent::RegisterInterest {
        task_id,
        fd,
        interest,
        waker,
    });
}

pub(crate) fn unregister_interests(fd: RawFd) {
    post_event(ReactorEvent::UnregisterInterests { fd });
}

pub(crate) fn init_task_local_state(
    task_id: TaskId,
    read_registrar: PyObject,
    write_registrar: PyObject,
) {
    let init_event = ReactorEvent::InstallRegistrars {
        task_id,
        read_registrar,
        write_registrar,
    };
    post_event(init_event);
}

pub(crate) fn cleanup_task_local_state(task_id: TaskId) {
    let cleanup_event = ReactorEvent::UninstallRegistrars { task_id };
    post_event(cleanup_event);
}

pub fn on_fd_ready(fd: i32, interest: Interest) {
    post_event(ReactorEvent::FdReady { fd, interest });
}
