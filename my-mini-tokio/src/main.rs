use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::thread::sleep;
use std::time::{Duration, Instant};

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


#[tokio::main]
async fn main() {
    println!("Hello, world! {:?}", Instant::now());
    let delay = Delay {
        when: Instant::now() + Duration::from_secs(10),
        waker: None,
    };
    delay.await;
    println!("Done {:?}", Instant::now());
}
