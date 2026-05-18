use bincode;
use riblt;
use std::hash::{DefaultHasher, Hash, Hasher};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct SimpleSymbol {
    unique_id: u64,
    timestamp: u64,
}

impl riblt::Symbol for SimpleSymbol {
    const BYTE_ARRAY_LENGTH: usize = 16;
    fn encode_to_bytes(&self) -> Vec<u8> {
        let mut buffer = vec![0u8; 16];
        buffer[0..8].copy_from_slice(&self.unique_id.to_le_bytes());
        buffer[8..16].copy_from_slice(&self.timestamp.to_le_bytes());
        buffer
    }
    fn decode_from_bytes(bytes: &Vec<u8>) -> Self {
        let unique_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let timestamp = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        SimpleSymbol { unique_id, timestamp }
    }
    fn hash_(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.unique_id.hash(&mut hasher);
        hasher.finish()
    }
}

fn main() {
    use std::collections::HashSet;

    let local_items: HashSet<SimpleSymbol> = HashSet::from([
        SimpleSymbol { unique_id: 7, timestamp: 0 },
        SimpleSymbol { unique_id: 15, timestamp: 0 },
        SimpleSymbol { unique_id: 16, timestamp: 0 },
        SimpleSymbol { unique_id: 17, timestamp: 0 }, // local only
    ]);

    let remote_items: HashSet<SimpleSymbol> = HashSet::from([
        SimpleSymbol { unique_id: 7, timestamp: 0 },
        SimpleSymbol { unique_id: 15, timestamp: 0 },
        SimpleSymbol { unique_id: 16, timestamp: 0 },
        SimpleSymbol { unique_id: 18, timestamp: 0 }, // remote only
    ]);

    // Encoder for the remote set (simulates the remote side).
    let mut remote_encoder = riblt::Encoder::new(remote_items);

    // Decoder knows the local set and will recover the symmetric difference.
    let mut decoder = riblt::Decoder::new();
    for item in &local_items {
        decoder.add_symbol(item.clone());
    }

    for i in 0..100 {
        println!("Consuming coded symbol {}", i);
        let cs = remote_encoder.get_coded_symbol(i);
        let encoded = bincode::serialize(&cs).unwrap();
        let decoded_cs: riblt::CodedSymbol<SimpleSymbol> = bincode::deserialize(&encoded).unwrap();

        decoder.add_coded_symbol(decoded_cs);
        decoder.try_decode();

        if decoder.decoded() {
            println!("Decoded after {} coded symbols", i + 1);
            for hs in decoder.remote_symbols() {
                println!("  remote-only: {:?}", hs.symbol);
            }
            for hs in decoder.local_symbols() {
                println!("  local-only:  {:?}", hs.symbol);
            }
            break;
        }
    }
}
