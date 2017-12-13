extern crate zmq;
extern crate rand;

use zmq::Context;

use rand::Rng;
use rand::distributions::{IndependentSample, Range};

use std::{time, thread};

fn main() {
    let mut context = Context::new();
    let mut server = context.socket(zmq::REP).unwrap();
    assert!(server.bind("tcp://*:5555").is_ok());

    let mut rng = rand::thread_rng();
    let period = Range::new(0, 3);

    let mut cycles = 0;
    loop {
        let request = server.recv_string(0)
            .expect("Unable to recv")
            .unwrap();
        cycles += 1;

        // Simulate various problems after a few cycles
        if cycles > 3 && period.ind_sample(&mut rng) == 0 {
            println!("I: simulating a crash");
            break;
        } else if cycles > 3 && period.ind_sample(&mut rng) == 0 {
            println!("I: simulating CPU overload");
            let two_secs = time::Duration::from_secs(2);
            thread::sleep(two_secs);
        }
        println!("I: normal request {}", request);
        thread::sleep(time::Duration::from_secs(1));
        server.send_str(&request, 0);
    }
}