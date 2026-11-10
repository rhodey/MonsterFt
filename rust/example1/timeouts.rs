use monsterft::{gate::GateOptions, proc::Options};
use std::time::Duration;

// Allow CI to override timeouts - CPUs can be much slower
fn ci() -> bool {
    std::env::var_os("MONSTERFT_CI").is_some_and(|value| value == "1")
}

pub fn gate_options() -> GateOptions {
    let mut options = GateOptions::default();

    if ci() {
        options.local_timeout = Duration::from_secs(10);
        options.apply_timeout = Duration::from_secs(10);
    }

    options
}

pub fn proc_options() -> Options {
    let mut options = Options::default();
    options.protocol.election_timeout = 100;

    if ci() {
        options.protocol.election_timeout = 2_000;
        options.protocol.append_timeout = 10_000;
        options.local_timeout = Duration::from_secs(10);
    }

    options
}
