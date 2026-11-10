use crate::error::{Code, Error, Result};
use rand_xoshiro::{
    Xoroshiro128Plus,
    rand_core::{Rng, SeedableRng},
};

/// Proc-local deterministic randomness. Every owner begins with the same full
/// 128-bit state; do not replace it with a smaller seed or an OS RNG here.
#[derive(Clone, Debug)]
pub struct Random {
    generator: Xoroshiro128Plus,
}

impl Random {
    pub fn new(seed: [u8; 16]) -> Result<Self> {
        if seed == [0; 16] {
            return Err(Error::new(
                Code::RpcIllegal,
                "seed must be a nonzero 16-byte value",
            ));
        }
        Ok(Self {
            generator: Xoroshiro128Plus::from_seed(seed),
        })
    }

    pub fn cid(&mut self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut cid = String::with_capacity(32);
        for word in [self.generator.next_u64(), self.generator.next_u64()] {
            for byte in word.to_le_bytes() {
                cid.push(HEX[(byte >> 4) as usize] as char);
                cid.push(HEX[(byte & 15) as usize] as char);
            }
        }
        cid
    }

    /// Sample both endpoints without modulo bias, using a fixed sampling rule
    /// shared by all Procs independently of the standard rand distribution API.
    pub fn int(&mut self, min: u64, max: u64) -> Result<u64> {
        if min > max {
            return Err(Error::new(
                Code::ArgumentIllegal,
                "random interval is illegal",
            ));
        }
        let Some(width) = (max - min).checked_add(1) else {
            return Ok(self.generator.next_u64());
        };
        let threshold = width.wrapping_neg() % width;
        loop {
            let value = self.generator.next_u64();
            if value >= threshold {
                return Ok(min + value % width);
            }
        }
    }
}
