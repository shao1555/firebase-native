//! Existence-filter bloom filters: the server sends a bloom
//! filter over the resource names of the documents a target
//! currently holds, so the client can tell which of its cached
//! documents are *definitely* no longer in the target's set
//! (and must be verified through limbo resolution) without a
//! full re-read.
//!
//! The membership probe follows the reference implementation:
//! MD5 of the document's resource name, split into two 64-bit
//! halves `h1`/`h2` (little-endian), probing bit positions
//! `(h1 + i * h2) % bit_count` for `i in 0..hash_count`. The
//! usable bit count is the bitmap's bit length minus its
//! padding (the trailing unused bits of the last byte).

/// A parsed bloom filter (the wire `BloomFilter` message).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct BloomFilter {
    bits: Vec<u8>,
    padding: usize,
    hash_count: usize,
}

impl BloomFilter {
    /// Parses the wire form. Returns `None` for shapes that
    /// cannot be probed (empty bitmap, padding beyond the bit
    /// length); callers treat an unusable filter as absent.
    pub(crate) fn new(bits: Vec<u8>, padding: i32, hash_count: i32) -> Option<Self> {
        if bits.is_empty() || padding < 0 || hash_count < 0 {
            return None;
        }
        let padding = padding as usize;
        if padding >= bits.len() * 8 {
            return None;
        }
        Some(Self {
            bits,
            padding,
            hash_count: hash_count as usize,
        })
    }

    /// The number of usable bits.
    fn bit_count(&self) -> usize {
        self.bits.len() * 8 - self.padding
    }

    fn bit_is_set(&self, index: usize) -> bool {
        self.bits[index / 8] & (1 << (index % 8)) != 0
    }

    /// Whether the resource name is *possibly* in the set.
    /// `false` is definitive (the document is not in the
    /// server's set); `true` may be a false positive.
    pub(crate) fn contains(&self, resource_name: &str) -> bool {
        if self.hash_count == 0 {
            return false;
        }
        let digest = md5(resource_name.as_bytes());
        let h1 = u64::from_le_bytes(digest[0..8].try_into().expect("md5 half"));
        let h2 = u64::from_le_bytes(digest[8..16].try_into().expect("md5 half"));
        let bit_count = self.bit_count() as u64;
        (0..self.hash_count as u64)
            .all(|i| self.bit_is_set((h1.wrapping_add(i.wrapping_mul(h2)) % bit_count) as usize))
    }
}

/// MD5 (RFC 1321), implemented locally: the crate needs it
/// only for bloom filter probes and takes no hashing
/// dependency for it.
fn md5(input: &[u8]) -> [u8; 16] {
    const S: [u32; 64] = [
        7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 5, 9, 14, 20, 5, 9, 14, 20, 5,
        9, 14, 20, 5, 9, 14, 20, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 6, 10,
        15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
    ];
    const K: [u32; 64] = [
        0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613,
        0xfd469501, 0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193,
        0xa679438e, 0x49b40821, 0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d,
        0x02441453, 0xd8a1e681, 0xe7d3fbc8, 0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
        0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a, 0xfffa3942, 0x8771f681, 0x6d9d6122,
        0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70, 0x289b7ec6, 0xeaa127fa,
        0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665, 0xf4292244,
        0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
        0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb,
        0xeb86d391,
    ];
    let mut a0: u32 = 0x67452301;
    let mut b0: u32 = 0xefcdab89;
    let mut c0: u32 = 0x98badcfe;
    let mut d0: u32 = 0x10325476;

    let mut message = input.to_vec();
    let bit_len = (input.len() as u64).wrapping_mul(8);
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_len.to_le_bytes());

    for chunk in message.chunks_exact(64) {
        let mut m = [0u32; 16];
        for (i, word) in m.iter_mut().enumerate() {
            *word = u32::from_le_bytes(chunk[i * 4..i * 4 + 4].try_into().expect("md5 word"));
        }
        let (mut a, mut b, mut c, mut d) = (a0, b0, c0, d0);
        for i in 0..64 {
            let (f, g) = match i / 16 {
                0 => ((b & c) | (!b & d), i),
                1 => ((d & b) | (!d & c), (5 * i + 1) % 16),
                2 => (b ^ c ^ d, (3 * i + 5) % 16),
                _ => (c ^ (b | !d), (7 * i) % 16),
            };
            let sum = f.wrapping_add(a).wrapping_add(K[i]).wrapping_add(m[g]);
            a = d;
            d = c;
            c = b;
            b = b.wrapping_add(sum.rotate_left(S[i]));
        }
        a0 = a0.wrapping_add(a);
        b0 = b0.wrapping_add(b);
        c0 = c0.wrapping_add(c);
        d0 = d0.wrapping_add(d);
    }
    let mut digest = [0u8; 16];
    digest[0..4].copy_from_slice(&a0.to_le_bytes());
    digest[4..8].copy_from_slice(&b0.to_le_bytes());
    digest[8..12].copy_from_slice(&c0.to_le_bytes());
    digest[12..16].copy_from_slice(&d0.to_le_bytes());
    digest
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(digest: &[u8; 16]) -> String {
        digest.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn md5_vectors() {
        assert_eq!(hex(&md5(b"")), "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(hex(&md5(b"abc")), "900150983cd24fb0d6963f7d28e17f72");
        assert_eq!(
            hex(&md5(b"The quick brown fox jumps over the lazy dog")),
            "9e107d9d372bb6826bd81d3542a419d6"
        );
    }
}

#[cfg(test)]
mod spec_vector_tests {
    use super::*;

    fn base64_decode(input: &str) -> Vec<u8> {
        let mut table = [255u8; 256];
        let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        for (i, c) in alphabet.iter().enumerate() {
            table[*c as usize] = i as u8;
        }
        let mut out = Vec::new();
        let mut acc: u32 = 0;
        let mut nbits = 0;
        for c in input.bytes() {
            if c == b'=' {
                break;
            }
            acc = (acc << 6) | table[c as usize] as u32;
            nbits += 6;
            if nbits >= 8 {
                nbits -= 8;
                out.push((acc >> nbits) as u8);
            }
        }
        out
    }

    #[test]
    fn spec_bitmap_membership() {
        // From the vendored existence-filter suite ("Bloom
        // filter can process special characters in document
        // name"): the driver's database is
        // `projects/test-project/databases/(default)`, and the
        // key must be a member of the filter built over it.
        let bits = base64_decode("IIAAIIAIIAAIIAIIAA==");
        let filter = BloomFilter::new(bits, 4, 10).expect("filter");
        let present = "projects/test-project/databases/(default)/documents/collection/ÀÒ∑";
        assert!(filter.contains(present));
        let absent = "projects/test-project/databases/(default)/documents/collection/not-there";
        assert!(!filter.contains(absent));
    }
}
