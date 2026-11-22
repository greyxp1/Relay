use crate::capture::Codec;
use davey::{DaveSession, MediaType, SigningKeyPair};
use std::{num::NonZeroU16, rc::Rc};
use zeroize::Zeroize;

struct SigningIdentity {
    user_id: u64,
    keys: SigningKeyPair,
}

impl Drop for SigningIdentity {
    fn drop(&mut self) {
        self.keys.private.zeroize();
    }
}

/// One ephemeral signing identity for all related voice and Go Live sessions.
#[derive(Clone)]
pub struct Identity(Rc<SigningIdentity>);

impl Identity {
    pub fn new(user: &str) -> Result<Self, String> {
        if user.len() > 20 || !user.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err("DAVE user ID must be a decimal uint64".into());
        }
        let user_id = user
            .parse()
            .map_err(|_| "DAVE user ID must be a decimal uint64")?;
        Ok(Self(Rc::new(SigningIdentity {
            user_id,
            keys: SigningKeyPair::generate(),
        })))
    }

    pub fn session(&self, group: u64, version: u16) -> Result<DaveSession, String> {
        let version = NonZeroU16::new(version).ok_or("DAVE requires encryption")?;
        DaveSession::new(version, self.0.user_id, group, Some(&self.0.keys))
            .map_err(|_| "DAVE initialization failed".into())
    }
}

pub fn media_codec(codec: Codec) -> (MediaType, davey::Codec) {
    match codec {
        Codec::Opus => (MediaType::AUDIO, davey::Codec::OPUS),
        Codec::H264 => (MediaType::VIDEO, davey::Codec::H264),
        Codec::H265 => (MediaType::VIDEO, davey::Codec::H265),
        Codec::Av1 => (MediaType::VIDEO, davey::Codec::AV1),
    }
}
