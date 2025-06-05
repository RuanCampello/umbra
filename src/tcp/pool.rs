//! Simple thread pool with lock from the [rust book](https://doc.rust-lang.org/book/ch21-02-multithreaded.html).
//! Would be better have a more robust implementation in the future.

use std::{
    num::NonZeroUsize,
    sync::{mpsc, Arc, Mutex},
    thread,
};

pub(in crate::tcp) struct ThreadPool {
    workers: Vec<Worker>,
    sender: Option<mpsc::Sender<Job>>,
}

struct Worker {
    id: usize,
    thread: Option<thread::JoinHandle<()>>,
}

type Job = Box<dyn FnOnce() + Send>;

impl ThreadPool {
    pub fn new(size: usize) -> Self {
        assert_ne!(size, 0, "ThreadPool cannot have zero workers");

        let (sender, receiver) = mpsc::channel();
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::with_capacity(size);

        (0..size).for_each(|id| workers.push(Worker::new(id, Arc::clone(&receiver))));

        Self {
            workers,
            sender: Some(sender),
        }
    }
    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);
        self.sender.as_ref().unwrap().send(job).unwrap();
    }
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<mpsc::Receiver<Job>>>) -> Self {
        let thread = thread::spawn(move || loop {
            let message = receiver.lock().unwrap().recv();

            match message {
                Ok(job) => job(),
                Err(_) => {
                    println!("Worker {id} disconnected. Shutting down.");
                    break;
                }
            }
        });

        Worker {
            id,
            thread: Some(thread),
        }
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        drop(self.sender.take());

        for worker in &mut self.workers {
            println!("Shutting down worker {}", worker.id);

            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}
