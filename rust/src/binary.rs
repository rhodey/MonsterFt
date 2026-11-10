//! MessagePack binary fields must not silently accept arrays of integers.
use bytes::Bytes;
use serde::{
    Deserialize, Deserializer,
    de::{self, Visitor},
};
use std::fmt;

struct Binary(Bytes);
impl<'de> Deserialize<'de> for Binary {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct BytesVisitor;
        impl<'de> Visitor<'de> for BytesVisitor {
            type Value = Binary;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("MessagePack binary")
            }
            fn visit_bytes<E: de::Error>(self, value: &[u8]) -> Result<Binary, E> {
                Ok(Binary(Bytes::copy_from_slice(value)))
            }
            fn visit_byte_buf<E: de::Error>(self, value: Vec<u8>) -> Result<Binary, E> {
                Ok(Binary(value.into()))
            }
        }
        decoder.deserialize_byte_buf(BytesVisitor)
    }
}
pub fn bytes<'de, D: Deserializer<'de>>(decoder: D) -> Result<Bytes, D::Error> {
    Binary::deserialize(decoder).map(|b| b.0)
}
pub fn vec<'de, D: Deserializer<'de>>(decoder: D) -> Result<Vec<u8>, D::Error> {
    struct VecVisitor;
    impl<'de> Visitor<'de> for VecVisitor {
        type Value = Vec<u8>;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("MessagePack binary")
        }
        fn visit_bytes<E: de::Error>(self, value: &[u8]) -> Result<Vec<u8>, E> {
            Ok(value.to_vec())
        }
        fn visit_byte_buf<E: de::Error>(self, value: Vec<u8>) -> Result<Vec<u8>, E> {
            Ok(value)
        }
    }
    decoder.deserialize_byte_buf(VecVisitor)
}

struct Hash([u8; 16]);
impl<'de> Deserialize<'de> for Hash {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct HashVisitor;
        impl<'de> Visitor<'de> for HashVisitor {
            type Value = Hash;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("MessagePack binary")
            }
            fn visit_bytes<E: de::Error>(self, value: &[u8]) -> Result<Hash, E> {
                value
                    .try_into()
                    .map(Hash)
                    .map_err(|_| de::Error::custom("hash must contain 16 bytes"))
            }
            fn visit_byte_buf<E: de::Error>(self, value: Vec<u8>) -> Result<Hash, E> {
                self.visit_bytes(&value)
            }
        }
        decoder.deserialize_bytes(HashVisitor)
    }
}
pub fn hash<'de, D: Deserializer<'de>>(decoder: D) -> Result<[u8; 16], D::Error> {
    Hash::deserialize(decoder).map(|hash| hash.0)
}
pub fn optional_hash<'de, D: Deserializer<'de>>(decoder: D) -> Result<Option<[u8; 16]>, D::Error> {
    Option::<Hash>::deserialize(decoder).map(|hash| hash.map(|hash| hash.0))
}
pub fn many<'de, D: Deserializer<'de>>(decoder: D) -> Result<Vec<Bytes>, D::Error> {
    Vec::<Binary>::deserialize(decoder).map(|v| v.into_iter().map(|b| b.0).collect())
}
pub fn optional_many<'de, D: Deserializer<'de>>(
    decoder: D,
) -> Result<Option<Vec<Bytes>>, D::Error> {
    Option::<Vec<Binary>>::deserialize(decoder)
        .map(|v| v.map(|v| v.into_iter().map(|b| b.0).collect()))
}
