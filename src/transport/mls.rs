//! Per-connection DAVE state.
use super::USERS;
use crate::{capture::Codec, dave};
use davey::{DaveSession, MediaType, ProposalsOperationType};
use std::collections::BTreeMap;
use tokio::time::Instant;

pub(super) struct Mls {
    pub session: DaveSession,
    scratch: Vec<u8>,
    roster: BTreeMap<u64, Vec<u8>>,
    pub pending: Option<(u16, Instant)>,
    pub external: bool,
    pub ready: bool,
    pub generation: u64,
}

impl Mls {
    pub fn new(
        identity: &dave::Identity,
        group: u64,
        version: u16,
        external: Option<&[u8]>,
    ) -> Result<Self, String> {
        let mut mls = Self {
            session: identity.session(group, version)?,
            scratch: Vec::new(),
            roster: BTreeMap::new(),
            pending: None,
            external: false,
            ready: false,
            generation: 0,
        };
        if let Some(sender) = external {
            mls.set_external_sender(sender)?;
        }
        Ok(mls)
    }

    pub fn set_external_sender(&mut self, sender: &[u8]) -> Result<(), String> {
        self.session
            .set_external_sender(sender)
            .map_err(|_| "Invalid MLS external sender")?;
        self.external = true;
        Ok(())
    }

    pub fn process_proposals(
        &mut self,
        data: &[u8],
        recognized: &[u64],
    ) -> Result<Option<Vec<u8>>, String> {
        let (operation, proposals) = data.split_first().ok_or("Empty MLS proposals")?;
        let operation = match operation {
            0 => ProposalsOperationType::APPEND,
            1 => ProposalsOperationType::REVOKE,
            _ => return Err("Invalid MLS proposals operation".into()),
        };
        let commit = self
            .session
            .process_proposals(operation, proposals, Some(recognized))
            .map_err(|_| "MLS proposals rejected")?;
        Ok(commit.map(|mut update| {
            if let Some(welcome) = update.welcome {
                update.commit.extend(welcome);
            }
            update.commit
        }))
    }

    pub fn update_roster(&mut self, recognized: &[u64]) -> Result<(), String> {
        let group = self.session.group().ok_or("DAVE roster missing")?;
        let mut roster = BTreeMap::new();
        for member in group.members() {
            if roster.len() >= USERS {
                return Err("DAVE roster exceeded limit".into());
            }
            let bytes: [u8; 8] = member
                .credential
                .serialized_content()
                .try_into()
                .map_err(|_| "Malformed DAVE member ID")?;
            let id = u64::from_be_bytes(bytes);
            if self.roster.get(&id) != Some(&member.signature_key) && !recognized.contains(&id) {
                return Err("Unrecognized DAVE member".into());
            }
            roster.insert(id, member.signature_key);
        }
        self.roster = roster;
        Ok(())
    }

    pub fn prepare(&mut self, transition: u16) -> Result<(), String> {
        if !self.external || self.pending.is_some() {
            return Err("Invalid DAVE prepare transition".into());
        }
        self.session
            .prepare_transition(transition, self.session.protocol_version().get())
            .map_err(|_| "DAVE receiver transition failed")?;
        if transition == 0 {
            self.finish_transition();
        } else {
            self.pending = Some((transition, Instant::now()));
        }
        Ok(())
    }

    pub fn execute(&mut self, transition: u16) -> Result<(), String> {
        if self.pending.map(|pending| pending.0) != Some(transition) {
            return Err("Invalid DAVE execute transition".into());
        }
        if !self
            .session
            .execute_transition(transition)
            .map_err(|_| "DAVE sender transition failed")?
        {
            return Err("Invalid DAVE execute transition".into());
        }
        self.pending = None;
        self.finish_transition();
        Ok(())
    }

    fn finish_transition(&mut self) {
        self.ready = self.session.can_encrypt();
        if self.ready {
            self.generation = self.generation.wrapping_add(1);
        }
    }

    pub fn encrypt(&mut self, codec: Codec, frame: &[u8]) -> Result<&[u8], String> {
        if frame.is_empty() {
            return Err("Empty DAVE frame".into());
        }
        let (media, codec) = dave::media_codec(codec);
        self.session
            .encrypt_into(media, codec, frame, &mut self.scratch)
            .map_err(|_| "DAVE frame encryption failed")?;
        Ok(&self.scratch)
    }

    pub fn decrypt_voice(&mut self, user: u64, frame: &[u8], header: [u8; 12]) -> Option<Vec<u8>> {
        if frame.is_empty() {
            return None;
        }
        let mut packet = vec![0; header.len() + frame.len()];
        let len = self
            .session
            .decrypt_into(user, MediaType::AUDIO, frame, &mut packet[header.len()..])
            .ok()?;
        if len == 0 {
            return None;
        }
        packet[..header.len()].copy_from_slice(&header);
        packet.truncate(header.len() + len);
        Some(packet)
    }
}
