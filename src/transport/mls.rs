//! Per-connection DAVE state.
use super::USERS;
use crate::dave;
use std::collections::{BTreeMap, BTreeSet};
use tokio::time::Instant;

pub(super) struct Mls {
    pub(super) session: dave::Session,
    pub(super) encryptor: dave::Encryptor,
    pub(super) decryptors: BTreeMap<String, dave::Decryptor>,
    pub(super) roster: BTreeSet<String>,
    pub(super) pending: Option<(u16, Instant)>,
    pub(super) external: bool,
    pub(super) ready: bool,
    pub(super) generation: u64,
}
impl Mls {
    pub(super) fn new(
        identity: &dave::Identity,
        group: u64,
        version: u16,
        external: Option<&[u8]>,
    ) -> Result<Self, String> {
        let session = dave::Session::new(identity, group, version)
            .map_err(|_| "DAVE initialization failed")?;
        let encryptor = session
            .encryptor()
            .map_err(|_| "DAVE encryptor initialization failed")?;
        let mut mls = Self {
            session,
            encryptor,
            decryptors: BTreeMap::new(),
            roster: BTreeSet::new(),
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
    pub(super) fn set_external_sender(&mut self, sender: &[u8]) -> Result<(), String> {
        self.session
            .set_external_sender(sender)
            .map_err(|_| "Invalid MLS external sender")?;
        self.external = true;
        Ok(())
    }
    pub(super) fn roster(
        &mut self,
        members: Vec<dave::RosterMember>,
        full: bool,
        recognized: &BTreeSet<String>,
        own: &str,
        receiving: bool,
    ) -> Result<(), String> {
        if full {
            self.roster.clear();
        }
        if members.len() > USERS {
            return Err("DAVE roster exceeded limit".into());
        }
        for member in members {
            let user = member.user_id.to_string();
            if !full && member.signature.is_empty() {
                self.roster.remove(&user);
            } else {
                if !recognized.contains(&user) {
                    return Err("Unrecognized DAVE member".into());
                }
                self.roster.insert(user);
            }
        }
        if self.roster.len() > USERS {
            return Err("DAVE roster exceeded limit".into());
        }
        if !receiving {
            return Ok(());
        }
        self.decryptors.retain(|user, _| self.roster.contains(user));
        for user in &self.roster {
            if user == own {
                continue;
            }
            if !self.decryptors.contains_key(user) {
                self.decryptors.insert(
                    user.clone(),
                    self.session
                        .decryptor(user)
                        .map_err(|_| "DAVE receiver creation failed")?,
                );
            }
            self.session
                .transition_receiver(self.decryptors.get_mut(user).unwrap())
                .map_err(|_| "DAVE receiver transition failed")?;
        }
        Ok(())
    }
    pub(super) fn execute(&mut self, transition: u16) -> Result<(), String> {
        if !self.external || self.pending.map(|p| p.0) != Some(transition) {
            return Err("Invalid DAVE execute transition".into());
        }
        // An epoch reset can execute before a new group has any authenticated
        // members. There is no sender ratchet until a commit or welcome arrives.
        if self.roster.is_empty() {
            self.pending = None;
            self.ready = false;
            return Ok(());
        }
        self.session
            .transition_sender(&mut self.encryptor)
            .map_err(|_| "DAVE sender transition failed")?;
        self.pending = None;
        self.generation = self.generation.wrapping_add(1);
        self.ready = true;
        Ok(())
    }
}
