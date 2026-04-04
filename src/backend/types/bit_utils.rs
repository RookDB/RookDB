//! Bit-string packing utilities for `BIT(n)` column storage.
//!
//! Bits are packed **most-significant-bit first** within each byte: the first
//! character of the bit string maps to bit 7 of byte 0, the second to bit 6,
//! and so on. This matches standard network/SQL bit-string conventions.

/// Pack a binary string (e.g. `"10110011"`) into a compact byte vector.
///
/// Each character must be `'0'` or `'1'`. Characters other than `'1'` are
/// treated as `0`. The output is `ceil(bits.len() / 8)` bytes long; any
/// trailing bits in the last byte are left as `0`.
pub(crate) fn pack_bit_string(bits: &str) -> Vec<u8> {
    let bit_count = bits.len();
    let byte_count = bit_count.div_ceil(8);
    let mut out = vec![0u8; byte_count];

    for (i, ch) in bits.chars().enumerate() {
        if ch == '1' {
            let byte_idx = i / 8;
            let bit_in_byte = i % 8;
            // MSB-first: first character → bit 7 of byte 0
            out[byte_idx] |= 1 << (7 - bit_in_byte);
        }
    }

    out
}

/// Unpack `bit_count` bits from a byte slice back into a `'0'`/`'1'` string.
///
/// The inverse of [`pack_bit_string`]. Reads exactly `bit_count` bits in
/// MSB-first order; extra bits beyond `bit_count` in the last byte are ignored.
pub(crate) fn unpack_bit_string(bytes: &[u8], bit_count: usize) -> String {
    let mut out = String::with_capacity(bit_count);
    for i in 0..bit_count {
        let byte_idx = i / 8;
        let bit_in_byte = i % 8;
        let is_one = (bytes[byte_idx] & (1 << (7 - bit_in_byte))) != 0;
        out.push(if is_one { '1' } else { '0' });
    }
    out
}

/// Normalize a SQL bit-string literal into a plain `'0'`/`'1'` string.
///
/// Accepts both `B'...'` / `b'...'` syntax and bare digit strings,
/// stripping the surrounding delimiter characters.
///
/// # Examples
/// ```text
/// normalize_bit_literal("B'1010'") → "1010"
/// normalize_bit_literal("1010")    → "1010"
/// ```
pub(crate) fn normalize_bit_literal(input: &str) -> String {
    let s = input.trim();
    if s.len() >= 3 && (s.starts_with("B'") || s.starts_with("b'")) && s.ends_with('"') {
        // Unreachable for single-quoted syntax but retained defensively.
        s[2..s.len() - 1].to_string()
    } else if s.len() >= 3 && (s.starts_with("B'") || s.starts_with("b'")) && s.ends_with('\'') {
        s[2..s.len() - 1].to_string()
    } else {
        s.trim_matches('\'').to_string()
    }
}
