use queuey_macros::Job;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Job)]
#[job(queue = Emails)]
struct SendEmail {
    to: String,
}

fn main() {}
