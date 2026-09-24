use std::cell::{RefCell};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{Sender, Receiver, channel};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::thread::sleep;
use std::time::{Duration, Instant};
use futures::future::BoxFuture;
use futures::task::{self, ArcWake};

struct Delay {
    when: Instant,
    waker: Option<Arc<Mutex<Waker>>>,
}
impl Future for Delay {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if let Some(waker) = &self.waker {
            let mut waker = waker.lock().unwrap();
            if !waker.will_wake(cx.waker()) {
                *waker = cx.waker().clone();
            }
        } else {
            let waker = Arc::new(Mutex::new(cx.waker().clone()));
            self.waker = Some(waker.clone());
            let when = self.when;
            thread::spawn(move || {
                    println!("timer running");
                    if when > Instant::now() {
                        sleep(when - Instant::now());
                    }

                    waker.lock().unwrap().wake_by_ref();

            });
        }

        if Instant::now() >= self.when {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

struct Task {
    future: Mutex<BoxFuture<'static, ()>>,
    executor: Sender<Arc<Task>>,
}

impl Task {
    fn spawn<F>(future: F, sender: Sender<Arc<Task>>)
    where F: Future<Output = ()> + Send + 'static {
        let task = Arc::new(Task {
            future: Mutex::new(Box::pin(future)),
            executor: sender.clone(),
        });
        let _ = sender.send(task);

    }

    fn poll(self: Arc<Self>) -> Poll<()> {
        let waker = task::waker(self.clone());
        let mut cx = Context::from_waker(&waker);
        self.future.lock().unwrap().as_mut().poll(&mut cx)
    }
}

impl ArcWake for Task {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let _ = arc_self.executor.send(arc_self.clone());
    }
}

thread_local! {
    static CURRENT: RefCell<Option<Sender<Arc<Task>>>> =
        RefCell::new(None);
}

struct MiniTokio {
    receiver: Receiver<Arc<Task>>,
    sender: Sender<Arc<Task>>,
}

impl MiniTokio {
    fn new() -> MiniTokio {
        let (tx, rx) = channel();
        MiniTokio {
            receiver: rx,
            sender: tx,
        }
    }
    fn spawn<F: Future<Output = ()> + Send + 'static>(&self, future: F) {
        Task::spawn(future, self.sender.clone());
    }
    fn run(&self) {
        CURRENT.with(|cell| {
            *cell.borrow_mut() = Some(self.sender.clone());
        });
        // why not combine run and new
        while let Ok(task) = self.receiver.recv() {
            let _ = task.poll();
        }
    }
}

fn spawn(future: impl Future<Output = ()> + 'static + Send) {
    CURRENT.with(|cell| {
        let sender = cell.borrow().clone().unwrap();
        Task::spawn(future, sender);
    })
}

async fn delay(dur: Duration) {
    let delay = Delay {
        when: Instant::now() + dur,
        waker: None,
    };
    delay.await;
}
#[tokio::main]
async fn main() {
    // Create the mini-tokio instance.
    let mini_tokio = MiniTokio::new();

    // Spawn the root task. All other tasks are spawned from the context of this
    // root task. No work happens until `mini_tokio.run()` is called.
    mini_tokio.spawn(async {
        // Spawn a task
        spawn(async {
            // Wait for a little bit of time so that "world" is printed after
            // "hello"
            delay(Duration::from_millis(100)).await;
            println!("world");
        });

        // Spawn a second task
        spawn(async {
            println!("hello");
        });

        // We haven't implemented executor shutdown, so force the process to exit.
        delay(Duration::from_millis(200)).await;
        std::process::exit(0);
    });

    // Start the mini-tokio executor loop. Scheduled tasks are received and
    // executed.
    mini_tokio.run();
}