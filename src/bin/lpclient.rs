extern crate zmq;

use zmq::Context;

const REQUEST_TIMEOUT: i64 = 2500;
const REQUEST_RETRIES: i32 = 3;
const SERVER_ENDPOINT: &str = "tcp://localhost:5555";

fn main() {
    let mut context = Context::new();
    println!("I: connecting to server...");
    let mut client = context.socket(zmq::REQ).unwrap();
    assert!(client.connect(SERVER_ENDPOINT).is_ok());

    let mut sequence = 0;
    let mut retries_left = REQUEST_RETRIES;
    while retries_left != 0 {
        println!("Looping");
        sequence += 1;
        let mut request = format!("{}", sequence);
        client.send_str(&request, 0);

        let mut expect_reply = 1;
        while expect_reply != 0 {
            let mut items = [ client.as_poll_item(zmq::POLLIN) ];
            let rc = zmq::poll(&mut items, REQUEST_TIMEOUT).unwrap();
            if rc == -1 {
                break;
            }

            if items[0].is_readable() {
                let reply = client.recv_string(0).unwrap().unwrap();
                let error_string = format!("E: malformed reply from server: {}", reply);
                let num: i32 = reply.parse()
                    .expect(&error_string);

                if num == sequence {
                    println!("I: server replied OK ({})", &reply);
                    retries_left = REQUEST_RETRIES;
                    expect_reply = 0;
                }
            } else {
                retries_left -= 1;
                if retries_left == 0 {
                    println!("E: server seems to be offline, abandoning");
                    break;
                } else {
                    println!("W: no response from server, retrying...");
                    let mut client = context.socket(zmq::REQ).unwrap();
                    assert!(client.connect(SERVER_ENDPOINT).is_ok());
                    client.send_str(&request, 0);
                }
            }
        }
    }
}