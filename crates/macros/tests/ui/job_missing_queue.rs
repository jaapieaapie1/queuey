use queuey_macros::Job;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Job)]
#[job(name = "send_email")]
struct SendEmail {
    to: String,
}

fn main() {}
