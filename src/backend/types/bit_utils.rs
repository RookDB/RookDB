pub(crate) fn pack_bit_string(bits: &str) -> Vec<u8> {
    let bit_count = bits.len();
    let byte_count = bit_count.div_ceil(8);
    let mut out = vec![0u8; byte_count];

    for (i, ch) in bits.chars().enumerate() {
        if ch == '1' {
            let byte_idx = i / 8;
            let bit_in_byte = i % 8;
            out[byte_idx] |= 1 << (7 - bit_in_byte);
        }
    }

    out
}

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
