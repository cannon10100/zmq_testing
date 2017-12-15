extern crate zmq;

#[macro_use]
extern crate lazy_static;

use zmq::Message;

use std::{time, str};
use std::vec::Vec;
use std::convert::AsMut;

const HEARTBEAT_LIVENESS: u64 = 3;
const HEARTBEAT_INTERVAL: u64 = 1000;

// Protocol constants
const PPP_READY: &str = "\001";      // Signals worker is ready
const PPP_HEARTBEAT: &str = "\002";  // Signals worker heartbeat

#[derive(Debug, PartialEq)]
struct Worker {
    identity: Message,
    id_string: String,
    expiry: time::Instant,
}

impl Worker {
    fn new(identity: Message) -> Worker {
        Worker {
            id_string: String::from(format!("{:?}", identity)),
            identity,
            expiry: time::Instant::now() +
                time::Duration::from_millis(HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS),
        }
    }
}

fn worker_ready<'a>(ready_worker: Worker, workers: &mut Vec<Worker>) {
    let mut index = 0;
    let mut rem_found = false;
    {
        let mut worker = workers.get(0);
        loop {
            if !worker.is_some() {
                break;
            }
            if worker.unwrap().id_string == ready_worker.id_string {
                rem_found = true;
                break;
            }
            index += 1;
            worker = workers.get(index);
        }
    }
    if rem_found {
        workers.remove(index);
    }

    workers.push(ready_worker)
}

fn worker_next(workers: &mut Vec<Worker>) -> Message {
    let worker = workers.pop().expect("No workers were available");
    worker.identity
}

fn workers_purge(workers: &mut Vec<Worker>) {
    let rem_positions = workers.iter()
        .position(|x| x.expiry < time::Instant::now());

    if rem_positions.is_some() {
        for pos in rem_positions {
            workers.remove(pos);
        }
    }
}

fn main() {
    let mut context = zmq::Context::new();
    let mut frontend = context.socket(zmq::ROUTER).unwrap();
    let mut backend = context.socket(zmq::ROUTER).unwrap();

    assert!(frontend.bind("tcp://*:5555").is_ok());
    assert!(backend.bind("tcp://*:5556").is_ok());

    let mut workers: Vec<Worker> = vec!();
    let mut heartbeat_at = time::Instant::now() +
        time::Duration::from_millis(HEARTBEAT_INTERVAL);

    loop {
        let mut items = [ backend.as_poll_item(zmq::POLLIN),
            frontend.as_poll_item(zmq::POLLIN) ];

        let rc = zmq::poll(&mut items, HEARTBEAT_INTERVAL as i64).unwrap();
        if rc == -1 {
            println!("E: Poll returned -1");
            break;
        }

        // Handle worker activity on backend
        if items[0].is_readable() {
            let mut msg = backend.recv_msg(0).unwrap();
            let more = msg.get_more();

            let mut worker = Worker::new(msg);
            worker_ready(worker, &mut workers);

            if more {
                let mut rest = backend.recv_multipart(0).unwrap();

                if rest.len() == 2 {
                    let worker_string = str::from_utf8(&rest[1]).unwrap();
                    if worker_string != PPP_READY && worker_string != PPP_HEARTBEAT {
                        println!("E: invalid message from worker: {}", worker_string);
                    }
                } else {
                    let rest_slice = rest.iter().map(|x| x.as_ref() as &[u8]).collect::<Vec<&[u8]>>();
                    frontend.send_multipart(&rest_slice, 0).unwrap();
                }
            }
        }

        // Handle client activity
        if items[1].is_readable() {
            let mut msg = frontend.recv_multipart(0).unwrap();
            let blank: Vec<u8> = vec!();
            let worker_identity = worker_next(&mut workers);

            // Routing
            msg.insert(0, blank);
            msg.insert(0, worker_identity.to_vec());

            let msg_slice: &[&[u8]] = &(msg.iter().map(|x| x.as_ref() as &[u8]).collect::<Vec<&[u8]>>());

            backend.send_multipart(msg_slice, 0);
        }

        if time::Instant::now() >= heartbeat_at {
            let mut worker = workers.first();
            let mut index = 0;

            loop {
                if !worker.is_some() {
                    break;
                }

                backend.send(&worker.unwrap().identity.to_vec(), zmq::SNDMORE);
                backend.send("".as_bytes(), zmq::SNDMORE);
                backend.send(PPP_HEARTBEAT.to_string().as_bytes(), 0);

                index += 1;
                worker = workers.get(index);
            }

            heartbeat_at = time::Instant::now() + time::Duration::from_millis(HEARTBEAT_INTERVAL);
        }

        workers_purge(&mut workers);
    }
    println!("Ending");
}