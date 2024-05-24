use rand::Rng;
use std::ops::Range;

pub fn generate_key(range: Range<u64>) -> [u8; 32] {
    let random_num = rand::thread_rng().gen_range(range);
    ethers::core::utils::keccak256(random_num.to_string().as_bytes())
}

pub fn generate_value(min_len: u32, max_len: u32) -> Vec<u8> {
    let mut len = min_len;
    if min_len < max_len {
        len = rand::thread_rng().gen_range(min_len..max_len)
    }
    (0..len).map(|_| rand::thread_rng().gen()).collect()
}
