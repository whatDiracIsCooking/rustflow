//! Translated from `Xorshift` in `taskflow/utility/math.hpp` (Taskflow v4.1.0).
//!
//! The one random number the scheduler needs: which queue a worker tries to
//! steal from next, drawn once per failed steal.
//!
//! # Design
//!
//! No `rand`: a runtime crate has to be better tested than our own port of it
//! would be (the README's dependency policy), which a twelve-line recurrence
//! with a known-answer test is not. Nothing here is cryptographic either --
//! victim selection wants decorrelated low bits and nothing more, and
//! predicting the next victim buys a steal from an empty queue.

/// What a zero seed becomes: the 32-bit golden-ratio word. Zero is a fixed
/// point of every xorshift -- upstream calls such a seed "a degenerated
/// generator" -- and the executor seeds from a thread-id hash, not a choice.
const ZERO_SEED_32: u32 = 0x9E37_79B9;

/// `ZERO_SEED_32` for [`Xorshift64`]: the 64-bit golden-ratio word.
const ZERO_SEED_64: u64 = 0x9E37_79B9_7F4A_7C15;

/// Marsaglia's 32-bit xorshift, over one word of state.
/// ```
/// use rustflow::Xorshift32;
/// let mut rng = Xorshift32::new(12345);
/// let first = rng.next_u32();
/// assert_ne!(first, rng.next_u32());
/// // Same seed, same sequence: a scheduling bug stays reproducible.
/// assert_eq!(Xorshift32::new(12345).next_u32(), first);
/// ```
#[derive(Debug, Clone)]
pub struct Xorshift32 {
    state: u32,
}

impl Xorshift32 {
    /// Creates a generator from `seed`, folding zero to `ZERO_SEED_32`.
    #[must_use]
    pub const fn new(seed: u32) -> Self {
        Self {
            state: if seed == 0 { ZERO_SEED_32 } else { seed },
        }
    }

    /// Draws the next value. The shift triple `13, 17, 5` is Marsaglia's, one
    /// of those giving this width its full period of `2^32 - 1`, so it is no
    /// place for nicer numbers; the name keeps it from reading as an
    /// [`Iterator`].
    pub fn next_u32(&mut self) -> u32 {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 17;
        self.state ^= self.state << 5;
        self.state
    }
}

/// Marsaglia's 64-bit xorshift with upstream's multiplicative output stage.
/// ```
/// use rustflow::Xorshift64;
/// let mut rng = Xorshift64::new(12345);
/// assert_ne!(rng.next_u64(), rng.next_u64());
/// ```
#[derive(Debug, Clone)]
pub struct Xorshift64 {
    state: u64,
}

/// Upstream's output multiplier: one odd multiply mixes a bare xorshift's high
/// bits down over its visibly weak low ones.
const OUTPUT_MULTIPLIER: u64 = 0x2545_F491_4F6C_DD1D;

impl Xorshift64 {
    /// Creates a generator from `seed`, folding zero to `ZERO_SEED_64`.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self {
            state: if seed == 0 { ZERO_SEED_64 } else { seed },
        }
    }

    /// Draws the next value. The shift triple is `13, 7, 17` here, not the
    /// 32-bit one: each width has its own full-period triples.
    pub fn next_u64(&mut self) -> u64 {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        // Wrapping: a mixing step, not arithmetic, as upstream's product is.
        self.state.wrapping_mul(OUTPUT_MULTIPLIER)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    /// A known answer, hand-computed from the recurrence, so that a mistyped
    /// shift constant fails here rather than quietly costing the scheduler its
    /// period. Seed 1 is the easiest case to redo by hand: `1 ^ (1 << 13)` is
    /// `0x2001`, whose top 15 bits are zero, so the right shift contributes
    /// nothing and the last step is `0x2001 ^ (0x2001 << 5)`.
    #[test]
    fn the_32_bit_recurrence_is_marsaglias() {
        assert_eq!(Xorshift32::new(1).next_u32(), 0x0004_2021);
    }

    /// The same, for the 64-bit state and its output multiply: the state after
    /// one round is `0x4082_2041`, and the value returned is that times
    /// [`OUTPUT_MULTIPLIER`].
    #[test]
    fn the_64_bit_recurrence_carries_upstreams_output_stage() {
        assert_eq!(Xorshift64::new(1).next_u64(), 0xBAFA_CF62_4F01_C45D);
        assert_eq!(
            Xorshift64::new(1).next_u64(),
            0x4082_2041_u64.wrapping_mul(OUTPUT_MULTIPLIER)
        );
    }

    /// A known answer at the folded zero seed, where every step of the
    /// recurrence overlaps its own shifted copy.
    ///
    /// The two answers above are seed 1, which is the easiest case to redo by
    /// hand for the same reason it is the weakest one to assert: `1 << 13` and
    /// `1` share no bit, nor do the words either later step combines, so at
    /// that seed an `|` in place of any `^` is the *same* word. The
    /// golden-ratio seed is dense in both widths, so each step's exclusive-or
    /// genuinely clears bits as well as setting them, and a recurrence that
    /// only ever set them -- which would cost the generator its period, the
    /// state saturating -- parts company here. Hand-derivable the same way,
    /// just longer: the 64-bit state after one round is `0xDC1B_77AE_0BF3_4DAD`
    /// and the value returned is that times [`OUTPUT_MULTIPLIER`].
    #[test]
    fn the_recurrence_clears_bits_as_well_as_setting_them() {
        assert_eq!(Xorshift32::new(0).next_u32(), 0x510C_4619);

        assert_eq!(Xorshift64::new(0).next_u64(), 0x7403_D1A2_8C9A_2599);
        assert_eq!(
            Xorshift64::new(0).next_u64(),
            0xDC1B_77AE_0BF3_4DAD_u64.wrapping_mul(OUTPUT_MULTIPLIER)
        );
    }

    /// A zero seed is the one input that breaks the recurrence -- zero is a
    /// fixed point of every xorshift -- so it must not survive construction.
    #[test]
    fn a_zero_seed_does_not_degenerate() {
        let mut rng = Xorshift32::new(0);
        assert_ne!(rng.next_u32(), 0);
        assert_ne!(rng.next_u32(), 0);

        let mut rng = Xorshift64::new(0);
        assert_ne!(rng.next_u64(), 0);
        assert_ne!(rng.next_u64(), 0);
    }

    /// The state never reaches zero from a non-zero seed, which is the property
    /// the full period rests on, and the draws do not collide early.
    #[test]
    fn a_short_run_neither_stalls_nor_repeats() {
        const DRAWS: usize = 10_000;

        let mut rng = Xorshift32::new(0xDEAD_BEEF);
        let mut seen = HashSet::with_capacity(DRAWS);
        for _ in 0..DRAWS {
            assert!(seen.insert(rng.next_u32()), "the 32-bit sequence repeated");
        }

        let mut rng = Xorshift64::new(0xDEAD_BEEF_CAFE_F00D);
        let mut seen = HashSet::with_capacity(DRAWS);
        for _ in 0..DRAWS {
            assert!(seen.insert(rng.next_u64()), "the 64-bit sequence repeated");
        }
    }

    /// Victim selection takes a remainder of a draw, so the low bits are the
    /// ones that matter: a generator whose `% n` never reaches some residue
    /// would leave a queue permanently unstolen-from.
    #[test]
    fn the_low_bits_cover_every_residue() {
        for queues in 2..=16_u32 {
            let mut rng = Xorshift32::new(queues);
            let mut seen = HashSet::new();
            for _ in 0..1000 {
                seen.insert(rng.next_u32() % queues);
            }
            assert_eq!(
                seen.len(),
                usize::try_from(queues).expect("a small count fits in a usize"),
                "{queues} queues were not all reachable"
            );
        }
    }
}
