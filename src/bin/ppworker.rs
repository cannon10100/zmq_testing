extern crate zmq;
extern crate rand;

use zmq::{Context, Socket};

use std::{thread, time, str};

use rand::distributions::{IndependentSample, Range};

const HEARTBEAT_LIVENESS: u64 = 3;
const HEARTBEAT_INTERVAL: u64 = 1000;
const INTERVAL_INIT: u64 = 1000; // Initial reconnect
const INTERVAL_MAX: u64 = 32000; // After exponential backoff

// Protocol constants
const PPP_READY: &str = "\001";      // Signals worker is ready
const PPP_HEARTBEAT: &str = "\002";  // Signals worker heartbeat

// Helper function that returns a new configured socket connected
// to the Paranoid Pirate queue
fn worker_socket(context: &Context) -> Socket {
    let worker = context.socket(zmq::DEALER).unwrap();
    assert!(worker.connect("tcp://localhost:5556").is_ok());

    // Tell queue we're ready for work
    println!("I: worker ready!");
    worker.send(PPP_READY.as_bytes(), 0).unwrap();

    worker
}

fn main() {
    let context = zmq::Context::new();
    let worker = worker_socket(&context);

    // If liveness hits zero, queue is considered disconnected
    let mut liveness = HEARTBEAT_LIVENESS;
    let mut interval = INTERVAL_INIT;

    // Send out heartbeats at regular intervals
    let heartbeat_at = time::Instant::now() + time::Duration::from_millis(HEARTBEAT_INTERVAL);
    let mut cycles = 0;
    let mut rng = rand::thread_rng();
    let mut range = Range::new(0, 5);

    loop {
        let mut items = [ worker.as_poll_item(zmq::POLLIN) ];
        let rc = zmq::poll(&mut items, HEARTBEAT_INTERVAL as i64).unwrap();
        if rc == -1 {
            break;
        }

        if items[0].is_readable() {
            // Get message
            // - 3-part envelope + content -> request
            // - 1-part HEARTBEAT -> heartbeat
            let msg = worker.recv_multipart(0).unwrap();

            if msg.len() == 4 {
                cycles += 1;

                if cycles > 3 && range.ind_sample(&mut rng) == 0 {
                    println!("I: simulating a crash");
                    break;
                } else if cycles > 3 && range.ind_sample(&mut rng) == 0 {
                    println!("I: simulating CPU overload");
                    thread::sleep(time::Duration::from_secs(3));
                }

                println!("I: normal reply");
                let msg_slice: &[&[u8]] = &(msg.iter().map(|x| x.as_ref() as &[u8]).collect::<Vec<&[u8]>>());
                worker.send_multipart(&msg_slice[1..], 0);
                liveness = HEARTBEAT_LIVENESS;
                thread::sleep(time::Duration::from_secs(1));
            } else {
                let first_str = str::from_utf8(&msg[1]).unwrap();
                if first_str == PPP_HEARTBEAT {
                    liveness = HEARTBEAT_LIVENESS;
                } else {
                    println!("E: invalid message {:?}", msg);
                }
            }

            interval = INTERVAL_INIT;
        } else {
            liveness -= 1;
            if liveness == 0 {
                println!("W: heartbeat failure, can't reach queue");
                println!("W: reconnecting in {} msec...", interval);
                thread::sleep_ms(interval as u32);

                if interval < INTERVAL_MAX {
                    interval *= 2;
                } else {
                    break;
                }
                let worker = worker_socket(&context);
                liveness = HEARTBEAT_LIVENESS;
            }
        }

        // Send heartbeat to queue if it's time
        if time::Instant::now() > heartbeat_at {
            let heartbeat_at = time::Instant::now() + time::Duration::from_millis(HEARTBEAT_INTERVAL);
            println!("I: worker hearbeat");
            worker.send_str(PPP_HEARTBEAT, 0);
        }
    }
}