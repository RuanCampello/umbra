//! CRC32C (Castagnoli), the checksum of log frames, checkpoint images and manifests
//! Hardware path through SSE4.2 when the CPU has it, slicing-by-8 tables otherwise

const POLYNOMIAL: u32 = 0x82F6_3B78;
static TABLE: [[u32; 256]; 8] = table();

#[inline]
pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    crc32c_append(0, bytes)
}

/// continues `crc` over `bytes`, so `crc32c_append(crc32c(a), b) == crc32c(a | b)`
#[inline]
pub(crate) fn crc32c_append(crc: u32, bytes: &[u8]) -> u32 {
    #[cfg(target_arch = "x86_64")]
    if std::is_x86_feature_detected!("sse4.2") {
        // SAFETY: the instruction set was detected at runtime
        return unsafe { hardware(crc, bytes) };
    }

    software(crc, bytes)
}

fn software(crc: u32, bytes: &[u8]) -> u32 {
    let mut crc = !crc;
    let (chunks, tail) = bytes.as_chunks::<8>();

    for chunk in chunks {
        let low = u32::from_le_bytes(chunk[..4].try_into().unwrap()) ^ crc;
        let high = u32::from_le_bytes(chunk[4..].try_into().unwrap());

        crc = TABLE[7][(low & 0xFF) as usize]
            ^ TABLE[6][(low >> 8 & 0xFF) as usize]
            ^ TABLE[5][(low >> 16 & 0xFF) as usize]
            ^ TABLE[4][(low >> 24) as usize]
            ^ TABLE[3][(high & 0xFF) as usize]
            ^ TABLE[2][(high >> 8 & 0xFF) as usize]
            ^ TABLE[1][(high >> 16 & 0xFF) as usize]
            ^ TABLE[0][(high >> 24) as usize];
    }

    for &byte in tail {
        crc = TABLE[0][((crc ^ byte as u32) & 0xFF) as usize] ^ (crc >> 8);
    }

    !crc
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn hardware(crc: u32, bytes: &[u8]) -> u32 {
    use std::arch::x86_64::{_mm_crc32_u64, _mm_crc32_u8};

    let mut wide = !crc as u64;
    let (chunks, tail) = bytes.as_chunks::<8>();

    for chunk in chunks {
        wide = _mm_crc32_u64(wide, u64::from_le_bytes(*chunk));
    }

    let mut crc = wide as u32;
    tail.iter().for_each(|&byte| crc = _mm_crc32_u8(crc, byte));

    !crc
}

const fn table() -> [[u32; 256]; 8] {
    let mut table = [[0u32; 256]; 8];

    let mut idx = 0;
    while idx < 256 {
        let mut crc = idx as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = match crc & 1 {
                1 => (crc >> 1) ^ POLYNOMIAL,
                _ => crc >> 1,
            };
            bit += 1;
        }
        table[0][idx] = crc;
        idx += 1;
    }

    let mut idx = 0;
    while idx < 256 {
        let mut slice = 1;
        while slice < 8 {
            let previous = table[slice - 1][idx];
            table[slice][idx] = (previous >> 8) ^ table[0][(previous & 0xFF) as usize];
            slice += 1;
        }
        idx += 1;
    }

    table
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_reference_vectors() {
        let ascending: Vec<u8> = (0..32).collect();

        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(&[0x00; 32]), 0x8A91_36AA);
        assert_eq!(crc32c(&[0xFF; 32]), 0x62A8_AB43);
        assert_eq!(crc32c(&ascending), 0x46DD_794E);
        assert_eq!(crc32c(&[]), 0);
    }

    #[test]
    fn software_agrees_with_dispatch_for_every_length() {
        let bytes: Vec<u8> = (0..67u32).map(|i| (i * 31 + 7) as u8).collect();

        for len in 0..=bytes.len() {
            assert_eq!(
                software(0, &bytes[..len]),
                crc32c(&bytes[..len]),
                "len {len}"
            );
        }
    }

    #[test]
    fn appending_is_split_invariant() {
        let bytes: Vec<_> = (0..100u32).map(|i| (i * 13) as u8).collect();
        let whole = crc32c(&bytes);

        for split in 0..=bytes.len() {
            let (head, tail) = bytes.split_at(split);
            assert_eq!(crc32c_append(crc32c(head), tail), whole);
            assert_eq!(software(software(0, head), tail), whole);
        }
    }
}
