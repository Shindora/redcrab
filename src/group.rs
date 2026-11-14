//! Classic consumer-group coordinator: membership, rebalances and durably committed offsets.
//!
//! This node coordinates every group. Membership lives in memory only, so after a restart members
//! simply rejoin; committed offsets are fsynced to the data dir before a commit is acknowledged.

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::describe_groups_response::{DescribedGroup, DescribedGroupMember};
use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;
use kafka_protocol::messages::list_groups_response::ListedGroup;
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::{
    GroupId, HeartbeatRequest, JoinGroupRequest, JoinGroupResponse, SyncGroupRequest,
    SyncGroupResponse,
};
use kafka_protocol::protocol::StrBytes;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::broker::replace_file;

/// Longest group id accepted: committed offsets live in a file named after the hex-encoded id.
pub const MAX_GROUP_ID_BYTES: usize = 120;
const MAX_SESSION_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const CLASSIC: &str = "classic";

/// Lifecycle state of a classic group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// No members; committed offsets may remain.
    Empty,
    /// Waiting for every member to (re)join.
    PreparingRebalance,
    /// Joined; waiting for the leader's assignment.
    CompletingRebalance,
    /// Every member has its assignment.
    Stable,
}

impl State {
    /// The name Kafka tools print for this state.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            State::Empty => "Empty",
            State::PreparingRebalance => "PreparingRebalance",
            State::CompletingRebalance => "CompletingRebalance",
            State::Stable => "Stable",
        }
    }
}

/// A response that is ready now, or arrives once the group's rebalance gets far enough.
pub enum Reply<T> {
    /// Answer immediately.
    Ready(T),
    /// Answer when the rebalance progresses; the sender is dropped if the member is removed.
    Waiting(oneshot::Receiver<T>),
}

impl<T> Reply<T> {
    /// Waits for the response, falling back to `removed` if the member left while waiting.
    pub async fn resolve(self, removed: impl FnOnce() -> T) -> T {
        match self {
            Reply::Ready(response) => response,
            Reply::Waiting(response) => response.await.unwrap_or_else(|_| removed()),
        }
    }
}

/// The client behind a `JoinGroup`, as `DescribeGroups` reports it.
#[derive(Debug, Clone, Default)]
pub struct ClientInfo {
    /// Client id from the request header.
    pub id: StrBytes,
    /// Client address formatted `/ip`, as Kafka does.
    pub host: StrBytes,
}

impl ClientInfo {
    /// The client with header id `client_id`, connected from `peer`.
    #[must_use]
    pub fn new(client_id: Option<StrBytes>, peer: Option<SocketAddr>) -> ClientInfo {
        ClientInfo {
            id: client_id.unwrap_or_default(),
            host: peer.map_or_else(StrBytes::default, |peer| {
                StrBytes::from_string(format!("/{}", peer.ip()))
            }),
        }
    }
}

/// One committed offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Committed {
    /// Next offset the group will consume.
    pub offset: i64,
    /// Leader epoch the committer saw, or -1.
    pub leader_epoch: i32,
    /// Opaque client metadata.
    pub metadata: Option<StrBytes>,
}

/// A group's committed offsets keyed by topic name and partition.
pub type Offsets = BTreeMap<(StrBytes, i32), Committed>;

struct Member {
    instance_id: Option<StrBytes>,
    client: ClientInfo,
    protocol_type: StrBytes,
    protocols: Vec<(StrBytes, Bytes)>,
    session_timeout: Duration,
    rebalance_timeout: Duration,
    expires: Instant,
    joining: Option<oneshot::Sender<JoinGroupResponse>>,
    syncing: Option<oneshot::Sender<SyncGroupResponse>>,
    assignment: Bytes,
}

impl Member {
    fn supports(&self, protocol: &StrBytes) -> bool {
        self.protocols.iter().any(|(name, _)| name == protocol)
    }

    fn metadata(&self, protocol: Option<&StrBytes>) -> Bytes {
        self.protocols
            .iter()
            .find(|(name, _)| Some(name) == protocol)
            .map(|(_, metadata)| metadata.clone())
            .unwrap_or_default()
    }

    /// A member blocked on a join or sync response is kept until the rebalance resolves it.
    fn alive(&self, now: Instant) -> bool {
        self.joining.is_some() || self.syncing.is_some() || now < self.expires
    }
}

struct Group {
    id: StrBytes,
    state: State,
    generation: i32,
    protocol_type: Option<StrBytes>,
    protocol: Option<StrBytes>,
    leader: Option<StrBytes>,
    members: BTreeMap<StrBytes, Member>,
    /// Member ids handed out with `MEMBER_ID_REQUIRED`, until they rejoin or their session runs out.
    pending: HashMap<StrBytes, Instant>,
    rebalance_deadline: Option<Instant>,
}

impl Group {
    fn new(id: StrBytes) -> Group {
        Group {
            id,
            state: State::Empty,
            generation: 0,
            protocol_type: None,
            protocol: None,
            leader: None,
            members: BTreeMap::new(),
            pending: HashMap::new(),
            rebalance_deadline: None,
        }
    }

    /// Whether a member with these protocols can join without breaking the group's common protocol.
    fn accepts(
        &self,
        member_id: &StrBytes,
        protocol_type: &StrBytes,
        protocols: &[(StrBytes, Bytes)],
    ) -> bool {
        let others = self
            .members
            .iter()
            .filter(|(id, _)| *id != member_id)
            .map(|(_, member)| member);
        others.clone().all(|m| &m.protocol_type == protocol_type)
            && protocols
                .iter()
                .any(|(name, _)| others.clone().all(|m| m.supports(name)))
    }

    fn member(&mut self, member_id: &StrBytes, generation: i32) -> Result<&mut Member, i16> {
        let current = self.generation;
        let member = self
            .members
            .get_mut(member_id)
            .ok_or(ResponseError::UnknownMemberId.code())?;
        if generation != current {
            return Err(ResponseError::IllegalGeneration.code());
        }
        Ok(member)
    }

    fn touch(&mut self, member_id: &StrBytes, now: Instant) {
        if let Some(member) = self.members.get_mut(member_id) {
            member.expires = now + member.session_timeout;
        }
    }

    fn prepare_rebalance(&mut self, now: Instant) {
        match self.state {
            State::PreparingRebalance => return,
            State::CompletingRebalance => {
                for member in self.members.values_mut() {
                    if let Some(syncing) = member.syncing.take() {
                        let _ = syncing.send(sync_error(ResponseError::RebalanceInProgress.code()));
                    }
                }
            }
            State::Empty | State::Stable => {}
        }
        let timeout = self
            .members
            .values()
            .map(|m| m.rebalance_timeout)
            .max()
            .unwrap_or_default();
        self.rebalance_deadline = Some(now + timeout);
        self.state = State::PreparingRebalance;
        info!(group = %self.id, members = self.members.len(), "preparing rebalance");
    }

    /// Ends the join phase once every member and handed-out member id has joined, or the
    /// rebalance timeout has passed; members that did not rejoin by then are dropped.
    fn try_complete_join(&mut self, now: Instant) {
        if self.state != State::PreparingRebalance {
            return;
        }
        let everyone =
            self.pending.is_empty() && self.members.values().all(|m| m.joining.is_some());
        if !everyone
            && self
                .rebalance_deadline
                .is_some_and(|deadline| now < deadline)
        {
            return;
        }
        let id = &self.id;
        self.members.retain(|member, m| {
            let rejoined = m.joining.is_some();
            if !rejoined {
                info!(group = %id, %member, "member did not rejoin before the rebalance timeout");
            }
            rejoined
        });
        self.generation += 1;
        self.rebalance_deadline = None;
        if self.members.is_empty() {
            self.state = State::Empty;
            self.protocol = None;
            self.leader = None;
            info!(group = %self.id, generation = self.generation, "group empty");
            return;
        }
        if !self
            .leader
            .as_ref()
            .is_some_and(|l| self.members.contains_key(l))
        {
            self.leader = self.members.keys().next().cloned();
        }
        self.protocol_type = self
            .members
            .values()
            .next()
            .map(|m| m.protocol_type.clone());
        self.protocol = self.select_protocol();
        self.state = State::CompletingRebalance;
        info!(
            group = %self.id,
            generation = self.generation,
            members = self.members.len(),
            protocol = ?self.protocol,
            "join complete"
        );
        let responses: Vec<_> = self
            .members
            .keys()
            .map(|id| (id.clone(), self.join_response(id)))
            .collect();
        for (id, response) in responses {
            if let Some(member) = self.members.get_mut(&id) {
                member.assignment = Bytes::new();
                member.expires = now + member.session_timeout;
                if let Some(joining) = member.joining.take() {
                    let _ = joining.send(response);
                }
            }
        }
    }

    /// The protocol every member supports that most members list first among those candidates.
    fn select_protocol(&self) -> Option<StrBytes> {
        let candidates: Vec<&StrBytes> = self
            .members
            .values()
            .next()?
            .protocols
            .iter()
            .map(|(name, _)| name)
            .filter(|name| self.members.values().all(|m| m.supports(name)))
            .collect();
        let votes = |candidate: &StrBytes| {
            self.members
                .values()
                .filter(|m| {
                    m.protocols
                        .iter()
                        .map(|(name, _)| name)
                        .find(|name| candidates.contains(name))
                        == Some(candidate)
                })
                .count()
        };
        let mut best: Option<(&StrBytes, usize)> = None;
        for &candidate in &candidates {
            let votes = votes(candidate);
            if best.is_none_or(|(_, most)| votes > most) {
                best = Some((candidate, votes));
            }
        }
        best.map(|(name, _)| name.clone())
    }

    fn join_response(&self, member_id: &StrBytes) -> JoinGroupResponse {
        let members = if self.leader.as_ref() == Some(member_id) {
            self.members
                .iter()
                .map(|(id, m)| {
                    JoinGroupResponseMember::default()
                        .with_member_id(id.clone())
                        .with_group_instance_id(m.instance_id.clone())
                        .with_metadata(m.metadata(self.protocol.as_ref()))
                })
                .collect()
        } else {
            Vec::new()
        };
        JoinGroupResponse::default()
            .with_generation_id(self.generation)
            .with_protocol_type(self.protocol_type.clone())
            .with_protocol_name(self.protocol.clone())
            .with_leader(self.leader.clone().unwrap_or_default())
            .with_member_id(member_id.clone())
            .with_members(members)
    }

    fn synced(&self) -> SyncGroupResponse {
        SyncGroupResponse::default()
            .with_protocol_type(self.protocol_type.clone())
            .with_protocol_name(self.protocol.clone())
    }

    /// Applies the leader's assignment, making the group stable and answering waiting followers.
    fn assign(&mut self, assignments: &[SyncGroupRequestAssignment]) {
        let synced = self.synced();
        for (id, member) in &mut self.members {
            member.assignment = assignments
                .iter()
                .find(|a| &a.member_id == id)
                .map(|a| a.assignment.clone())
                .unwrap_or_default();
            if let Some(syncing) = member.syncing.take() {
                let _ = syncing.send(synced.clone().with_assignment(member.assignment.clone()));
            }
        }
        self.state = State::Stable;
        info!(group = %self.id, generation = self.generation, members = self.members.len(), "group stable");
    }

    fn members_left(&mut self, now: Instant) {
        if matches!(self.state, State::Stable | State::CompletingRebalance) {
            self.prepare_rebalance(now);
        }
        self.try_complete_join(now);
    }
}

/// Coordinator of every group: in-memory membership plus the durable offset store.
pub struct Coordinator {
    groups: Mutex<HashMap<StrBytes, Group>>,
    offsets: OffsetStore,
}

impl Coordinator {
    /// Opens the coordinator, loading committed offsets from `dir`. Blocking.
    ///
    /// # Errors
    /// Fails when the directory cannot be created or an offsets file cannot be read.
    pub fn open(dir: PathBuf) -> io::Result<Coordinator> {
        Ok(Coordinator {
            groups: Mutex::new(HashMap::new()),
            offsets: OffsetStore::open(dir)?,
        })
    }

    /// Handles `JoinGroup`: hands out member ids, starts rebalances and answers once the join
    /// phase completes.
    pub fn join(
        &self,
        request: &JoinGroupRequest,
        version: i16,
        client: ClientInfo,
        now: Instant,
    ) -> Reply<JoinGroupResponse> {
        let fail = |error: ResponseError| {
            Reply::Ready(join_error(error.code(), request.member_id.clone(), version))
        };
        let Some(session_timeout) =
            millis(request.session_timeout_ms).filter(|t| *t <= MAX_SESSION_TIMEOUT)
        else {
            return fail(ResponseError::InvalidSessionTimeout);
        };
        let rebalance_timeout = match version {
            0 => session_timeout,
            _ => millis(request.rebalance_timeout_ms).unwrap_or(session_timeout),
        };
        if !valid_group_id(&request.group_id) {
            return fail(ResponseError::InvalidGroupId);
        }
        if request.protocol_type.is_empty() || request.protocols.is_empty() {
            return fail(ResponseError::InconsistentGroupProtocol);
        }
        let protocols: Vec<_> = request
            .protocols
            .iter()
            .map(|p| (p.name.clone(), p.metadata.clone()))
            .collect();
        let mut groups = lock(&self.groups);
        let group = groups
            .entry(request.group_id.0.clone())
            .or_insert_with(|| Group::new(request.group_id.0.clone()));
        if !group.accepts(&request.member_id, &request.protocol_type, &protocols) {
            return fail(ResponseError::InconsistentGroupProtocol);
        }
        let member_id = if request.member_id.is_empty() {
            let id = StrBytes::from_string(format!("{}-{}", client.id, Uuid::new_v4()));
            if version >= 4 {
                group.pending.insert(id.clone(), now + session_timeout);
                return Reply::Ready(join_error(
                    ResponseError::MemberIdRequired.code(),
                    id,
                    version,
                ));
            }
            id
        } else if group.pending.remove(&request.member_id).is_some()
            || group.members.contains_key(&request.member_id)
        {
            request.member_id.clone()
        } else {
            return fail(ResponseError::UnknownMemberId);
        };
        if let Some(member) = group.members.get(&member_id) {
            let unchanged =
                member.protocol_type == request.protocol_type && member.protocols == protocols;
            let leads = group.leader.as_ref() == Some(&member_id);
            let current = match group.state {
                State::CompletingRebalance => unchanged,
                State::Stable => unchanged && !leads,
                State::Empty | State::PreparingRebalance => false,
            };
            if current {
                group.touch(&member_id, now);
                return Reply::Ready(group.join_response(&member_id));
            }
        }
        let (joining, joined) = oneshot::channel();
        // ponytail: group.instance.id is echoed but not honoured; static members rejoin as dynamic ones.
        let member = Member {
            instance_id: request.group_instance_id.clone(),
            client,
            protocol_type: request.protocol_type.clone(),
            protocols,
            session_timeout,
            rebalance_timeout,
            expires: now + session_timeout,
            joining: Some(joining),
            syncing: None,
            assignment: Bytes::new(),
        };
        group.leader.get_or_insert_with(|| member_id.clone());
        group.members.insert(member_id, member);
        group.prepare_rebalance(now);
        group.try_complete_join(now);
        Reply::Waiting(joined)
    }

    /// Handles `SyncGroup`: the leader's request carries every assignment, followers wait for it.
    pub fn sync(&self, request: &SyncGroupRequest, now: Instant) -> Reply<SyncGroupResponse> {
        let fail = |error: ResponseError| Reply::Ready(sync_error(error.code()));
        let mut groups = lock(&self.groups);
        let Some(group) = groups.get_mut(&request.group_id.0) else {
            return fail(ResponseError::UnknownMemberId);
        };
        match group.member(&request.member_id, request.generation_id) {
            Ok(member) => member.expires = now + member.session_timeout,
            Err(code) => return Reply::Ready(sync_error(code)),
        }
        let differs = |wanted: &Option<StrBytes>, actual: &Option<StrBytes>| {
            wanted.is_some() && wanted != actual
        };
        if differs(&request.protocol_type, &group.protocol_type)
            || differs(&request.protocol_name, &group.protocol)
        {
            return fail(ResponseError::InconsistentGroupProtocol);
        }
        let member = &request.member_id;
        match group.state {
            State::Empty | State::PreparingRebalance => fail(ResponseError::RebalanceInProgress),
            State::Stable => Reply::Ready(
                group.synced().with_assignment(
                    group
                        .members
                        .get(member)
                        .map(|m| m.assignment.clone())
                        .unwrap_or_default(),
                ),
            ),
            State::CompletingRebalance if group.leader.as_ref() == Some(member) => {
                group.assign(&request.assignments);
                Reply::Ready(
                    group.synced().with_assignment(
                        group
                            .members
                            .get(member)
                            .map(|m| m.assignment.clone())
                            .unwrap_or_default(),
                    ),
                )
            }
            State::CompletingRebalance => {
                let (syncing, synced) = oneshot::channel();
                if let Some(m) = group.members.get_mut(member) {
                    m.syncing = Some(syncing);
                }
                Reply::Waiting(synced)
            }
        }
    }

    /// Handles `Heartbeat`, returning its error code.
    pub fn heartbeat(&self, request: &HeartbeatRequest, now: Instant) -> i16 {
        let mut groups = lock(&self.groups);
        let Some(group) = groups.get_mut(&request.group_id.0) else {
            return ResponseError::UnknownMemberId.code();
        };
        match group.member(&request.member_id, request.generation_id) {
            Ok(member) => member.expires = now + member.session_timeout,
            Err(code) => return code,
        }
        if group.state == State::PreparingRebalance {
            ResponseError::RebalanceInProgress.code()
        } else {
            0
        }
    }

    /// Removes members from a group, returning one error code per member id.
    pub fn leave(&self, group_id: &StrBytes, member_ids: &[StrBytes], now: Instant) -> Vec<i16> {
        let mut groups = lock(&self.groups);
        let Some(group) = groups.get_mut(group_id) else {
            return vec![ResponseError::UnknownMemberId.code(); member_ids.len()];
        };
        let mut left = false;
        let codes = member_ids
            .iter()
            .map(|id| {
                if group.pending.remove(id).is_some() {
                    0
                } else if group.members.remove(id).is_some() {
                    info!(group = %group_id, member = %id, "member left");
                    left = true;
                    0
                } else {
                    ResponseError::UnknownMemberId.code()
                }
            })
            .collect();
        if left {
            group.members_left(now);
        }
        codes
    }

    /// Drops members whose session ran out and ends join phases whose rebalance timeout passed.
    pub fn expire(&self, now: Instant) {
        let mut groups = lock(&self.groups);
        for group in groups.values_mut() {
            group.pending.retain(|_, deadline| now < *deadline);
            let before = group.members.len();
            let id = &group.id;
            group.members.retain(|member, m| {
                let alive = m.alive(now);
                if !alive {
                    info!(group = %id, %member, "member session expired");
                }
                alive
            });
            if group.members.len() < before {
                group.members_left(now);
            } else {
                group.try_complete_join(now);
            }
        }
        groups.retain(|_, group| group.state != State::Empty || !group.pending.is_empty());
    }

    /// Checks that an `OffsetCommit` comes from a current member, or from outside the group
    /// (generation -1, no member id) while the group has no members.
    ///
    /// # Errors
    /// Returns the Kafka error code to report for every partition of the commit.
    pub fn check_commit(
        &self,
        group_id: &StrBytes,
        member_id: &StrBytes,
        generation: i32,
    ) -> Result<(), i16> {
        if !valid_group_id(group_id) {
            return Err(ResponseError::InvalidGroupId.code());
        }
        let mut groups = lock(&self.groups);
        let group = groups.get_mut(group_id);
        if generation < 0 && member_id.is_empty() {
            return match group {
                Some(group) if !group.members.is_empty() => {
                    Err(ResponseError::UnknownMemberId.code())
                }
                _ => Ok(()),
            };
        }
        let group = group.ok_or(ResponseError::UnknownMemberId.code())?;
        group.member(member_id, generation)?;
        match group.state {
            State::CompletingRebalance => Err(ResponseError::RebalanceInProgress.code()),
            _ => Ok(()),
        }
    }

    /// Durably records `offsets` once [`Coordinator::check_commit`] passes; returns the error code.
    pub async fn commit(
        &self,
        group_id: &StrBytes,
        member_id: &StrBytes,
        generation: i32,
        offsets: Vec<((StrBytes, i32), Committed)>,
    ) -> i16 {
        if let Err(code) = self.check_commit(group_id, member_id, generation) {
            return code;
        }
        if offsets.is_empty() {
            return 0;
        }
        match self.offsets.save(group_id, offsets).await {
            Ok(()) => 0,
            Err(e) => {
                error!(group = %group_id, "committing offsets failed: {e}");
                ResponseError::CoordinatorNotAvailable.code()
            }
        }
    }

    /// The group's committed offsets.
    pub async fn committed(&self, group_id: &StrBytes) -> Offsets {
        self.offsets.load(group_id).await
    }

    /// The group as `DescribeGroups` shows it, or `None` if it has neither members nor offsets.
    pub fn describe(&self, group_id: &StrBytes) -> Option<DescribedGroup> {
        let groups = lock(&self.groups);
        let described = DescribedGroup::default().with_group_id(GroupId(group_id.clone()));
        let Some(group) = groups.get(group_id) else {
            return self.offsets.contains(group_id).then(|| {
                described.with_group_state(StrBytes::from_static_str(State::Empty.name()))
            });
        };
        let stable = group.state == State::Stable;
        let members = group
            .members
            .iter()
            .map(|(id, m)| {
                let member = DescribedGroupMember::default()
                    .with_member_id(id.clone())
                    .with_group_instance_id(m.instance_id.clone())
                    .with_client_id(m.client.id.clone())
                    .with_client_host(m.client.host.clone());
                if stable {
                    member
                        .with_member_metadata(m.metadata(group.protocol.as_ref()))
                        .with_member_assignment(m.assignment.clone())
                } else {
                    member
                }
            })
            .collect();
        Some(
            described
                .with_group_state(StrBytes::from_static_str(group.state.name()))
                .with_protocol_type(group.protocol_type.clone().unwrap_or_default())
                .with_protocol_data(
                    group
                        .protocol
                        .clone()
                        .filter(|_| stable)
                        .unwrap_or_default(),
                )
                .with_members(members),
        )
    }

    /// Every group with members or committed offsets.
    pub fn list(&self) -> Vec<ListedGroup> {
        let groups = lock(&self.groups);
        let listed = |id: &StrBytes, state: State, protocol_type: Option<&StrBytes>| {
            ListedGroup::default()
                .with_group_id(GroupId(id.clone()))
                .with_protocol_type(protocol_type.cloned().unwrap_or_default())
                .with_group_state(StrBytes::from_static_str(state.name()))
                .with_group_type(StrBytes::from_static_str(CLASSIC))
        };
        let mut all: Vec<_> = groups
            .values()
            .map(|g| listed(&g.id, g.state, g.protocol_type.as_ref()))
            .collect();
        all.extend(
            self.offsets
                .group_ids()
                .iter()
                .filter(|id| !groups.contains_key(*id))
                .map(|id| listed(id, State::Empty, None)),
        );
        all
    }
}

/// `JoinGroup` error response. Before v7 the protocol name is not nullable.
pub fn join_error(code: i16, member_id: StrBytes, version: i16) -> JoinGroupResponse {
    JoinGroupResponse::default()
        .with_error_code(code)
        .with_generation_id(-1)
        .with_protocol_name((version < 7).then(StrBytes::default))
        .with_member_id(member_id)
}

/// `SyncGroup` error response.
pub fn sync_error(code: i16) -> SyncGroupResponse {
    SyncGroupResponse::default().with_error_code(code)
}

fn valid_group_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= MAX_GROUP_ID_BYTES
}

fn millis(ms: i32) -> Option<Duration> {
    u64::try_from(ms)
        .ok()
        .filter(|ms| *ms > 0)
        .map(Duration::from_millis)
}

// ponytail: one fsynced, atomically replaced file per group, rewritten whole on every commit; cost
// grows with the group's partition count. Moves onto the replicated log in a later milestone.
struct OffsetStore {
    dir: PathBuf,
    groups: Mutex<HashMap<StrBytes, Arc<tokio::sync::Mutex<Offsets>>>>,
}

impl OffsetStore {
    fn open(dir: PathBuf) -> io::Result<OffsetStore> {
        if !dir.is_dir() {
            fs::create_dir_all(&dir)?;
            if let Some(parent) = dir.parent() {
                File::open(parent)?.sync_all()?;
            }
        }
        let mut groups = HashMap::new();
        for entry in fs::read_dir(&dir)? {
            let path = entry?.path();
            if path.extension().is_some_and(|e| e == "tmp") {
                fs::remove_file(&path)?;
                continue;
            }
            let group = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(unhex)
                .and_then(|id| String::from_utf8(id).ok());
            let Some(group) = group else {
                warn!(?path, "ignoring unexpected file in groups dir");
                continue;
            };
            let offsets = parse_offsets(&fs::read_to_string(&path)?)?;
            info!(%group, partitions = offsets.len(), "loaded committed offsets");
            groups.insert(
                StrBytes::from_string(group),
                Arc::new(tokio::sync::Mutex::new(offsets)),
            );
        }
        Ok(OffsetStore {
            dir,
            groups: Mutex::new(groups),
        })
    }

    /// Merges `entries` into the group's offsets and makes them durable. The write and the
    /// in-memory update happen on one blocking task, so a dropped caller cannot leave them apart.
    async fn save(
        &self,
        group: &StrBytes,
        entries: Vec<((StrBytes, i32), Committed)>,
    ) -> io::Result<()> {
        let slot = Arc::clone(lock(&self.groups).entry(group.clone()).or_default());
        let mut offsets = slot.lock_owned().await;
        let dir = self.dir.clone();
        let name = hex(group.as_bytes());
        tokio::task::spawn_blocking(move || {
            let mut next = offsets.clone();
            next.extend(entries);
            replace_file(&dir, &name, render_offsets(&next).as_bytes())?;
            *offsets = next;
            Ok(())
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn load(&self, group: &StrBytes) -> Offsets {
        let slot = lock(&self.groups).get(group).cloned();
        match slot {
            Some(slot) => slot.lock().await.clone(),
            None => Offsets::new(),
        }
    }

    fn contains(&self, group: &StrBytes) -> bool {
        lock(&self.groups).contains_key(group)
    }

    fn group_ids(&self) -> Vec<StrBytes> {
        lock(&self.groups).keys().cloned().collect()
    }
}

/// One line per partition: topic, partition, offset, leader epoch, hex metadata or `-` for null.
fn render_offsets(offsets: &Offsets) -> String {
    let mut text = String::new();
    for ((topic, partition), committed) in offsets {
        let metadata = committed
            .metadata
            .as_ref()
            .map_or_else(|| "-".to_owned(), |m| hex(m.as_bytes()));
        let _ = writeln!(
            text,
            "{topic}\t{partition}\t{}\t{}\t{metadata}",
            committed.offset, committed.leader_epoch
        );
    }
    text
}

fn parse_offsets(text: &str) -> io::Result<Offsets> {
    text.lines()
        .map(|line| {
            parse_offset(line).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("bad line in offsets file: {line:?}"),
                )
            })
        })
        .collect()
}

fn parse_offset(line: &str) -> Option<((StrBytes, i32), Committed)> {
    let fields: Vec<&str> = line.split('\t').collect();
    let [topic, partition, offset, leader_epoch, metadata] = fields[..] else {
        return None;
    };
    let metadata = match metadata {
        "-" => None,
        hexed => Some(StrBytes::from_string(
            String::from_utf8(unhex(hexed)?).ok()?,
        )),
    };
    Some((
        (
            StrBytes::from_string(topic.to_owned()),
            partition.parse().ok()?,
        ),
        Committed {
            offset: offset.parse().ok()?,
            leader_epoch: leader_epoch.parse().ok()?,
            metadata,
        },
    ))
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut text, b| {
            let _ = write!(text, "{b:02x}");
            text
        })
}

fn unhex(text: &str) -> Option<Vec<u8>> {
    if text.len() % 2 != 0 || !text.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).ok())
        .collect()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;

    use super::*;

    const SESSION: Duration = Duration::from_secs(10);
    const REBALANCE: Duration = Duration::from_secs(30);

    fn s(text: &str) -> StrBytes {
        StrBytes::from_string(text.to_owned())
    }

    fn request(member: &StrBytes, protocols: &[&str]) -> JoinGroupRequest {
        JoinGroupRequest::default()
            .with_group_id(GroupId(s("g")))
            .with_session_timeout_ms(10_000)
            .with_rebalance_timeout_ms(30_000)
            .with_member_id(member.clone())
            .with_protocol_type(s("consumer"))
            .with_protocols(
                protocols
                    .iter()
                    .map(|name| {
                        JoinGroupRequestProtocol::default()
                            .with_name(s(name))
                            .with_metadata(Bytes::from(format!("{name}-metadata")))
                    })
                    .collect(),
            )
    }

    fn client() -> ClientInfo {
        ClientInfo {
            id: s("client"),
            host: s("/127.0.0.1"),
        }
    }

    fn coordinator() -> (tempfile::TempDir, Coordinator) {
        let dir = tempfile::tempdir().unwrap();
        let coordinator = Coordinator::open(dir.path().join("groups")).unwrap();
        (dir, coordinator)
    }

    fn ready<T>(reply: Reply<T>) -> T {
        match reply {
            Reply::Ready(response) => response,
            Reply::Waiting(mut response) => response.try_recv().expect("reply not ready yet"),
        }
    }

    fn waiting<T>(reply: Reply<T>) -> oneshot::Receiver<T> {
        match reply {
            Reply::Waiting(response) => response,
            Reply::Ready(_) => panic!("expected the reply to wait for the rebalance"),
        }
    }

    /// Joins as a new member (two round trips, as v4+ clients do) and returns the member id.
    fn new_member(
        coordinator: &Coordinator,
        protocols: &[&str],
        now: Instant,
    ) -> (StrBytes, Reply<JoinGroupResponse>) {
        let required = ready(coordinator.join(&request(&s(""), protocols), 9, client(), now));
        assert_eq!(required.error_code, ResponseError::MemberIdRequired.code());
        let id = required.member_id;
        let reply = coordinator.join(&request(&id, protocols), 9, client(), now);
        (id, reply)
    }

    fn heartbeat(
        coordinator: &Coordinator,
        member: &StrBytes,
        generation: i32,
        now: Instant,
    ) -> i16 {
        coordinator.heartbeat(
            &HeartbeatRequest::default()
                .with_group_id(GroupId(s("g")))
                .with_member_id(member.clone())
                .with_generation_id(generation),
            now,
        )
    }

    fn sync(
        coordinator: &Coordinator,
        member: &StrBytes,
        generation: i32,
        assignments: &[(&StrBytes, &str)],
        now: Instant,
    ) -> Reply<SyncGroupResponse> {
        let assignments = assignments
            .iter()
            .map(|(id, assignment)| {
                SyncGroupRequestAssignment::default()
                    .with_member_id((*id).clone())
                    .with_assignment(Bytes::from(assignment.to_string()))
            })
            .collect();
        coordinator.sync(
            &SyncGroupRequest::default()
                .with_group_id(GroupId(s("g")))
                .with_member_id(member.clone())
                .with_generation_id(generation)
                .with_assignments(assignments),
            now,
        )
    }

    fn state(coordinator: &Coordinator) -> Option<StrBytes> {
        coordinator.describe(&s("g")).map(|g| g.group_state)
    }

    /// A stable two-member group at generation 2 with `a` leading.
    fn stable_pair(coordinator: &Coordinator, now: Instant) -> (StrBytes, StrBytes) {
        let (a, joined) = new_member(coordinator, &["range"], now);
        assert_eq!(ready(joined).generation_id, 1);
        ready(sync(coordinator, &a, 1, &[(&a, "a1")], now));
        let (b, b_joined) = new_member(coordinator, &["range"], now);
        let b_joined = waiting(b_joined);
        let a_joined = ready(coordinator.join(&request(&a, &["range"]), 9, client(), now));
        assert_eq!(a_joined.generation_id, 2);
        let mut b_joined = b_joined;
        assert_eq!(b_joined.try_recv().unwrap().generation_id, 2);
        let pending = waiting(sync(coordinator, &b, 2, &[], now));
        ready(sync(coordinator, &a, 2, &[(&a, "a2"), (&b, "b2")], now));
        drop(pending);
        (a, b)
    }

    #[test]
    fn join_requires_member_id_then_single_member_leads() {
        let (_dir, coordinator) = coordinator();
        let now = Instant::now();
        let (id, joined) = new_member(&coordinator, &["range", "cooperative-sticky"], now);
        let joined = ready(joined);
        assert_eq!(joined.error_code, 0);
        assert_eq!(joined.generation_id, 1);
        assert_eq!(joined.leader, id);
        assert_eq!(joined.member_id, id);
        assert_eq!(joined.protocol_name, Some(s("range")));
        assert_eq!(joined.protocol_type, Some(s("consumer")));
        assert_eq!(joined.members.len(), 1);
        assert_eq!(joined.members[0].metadata, Bytes::from("range-metadata"));
        assert_eq!(state(&coordinator), Some(s("CompletingRebalance")));

        let synced = ready(sync(&coordinator, &id, 1, &[(&id, "p0,p1")], now));
        assert_eq!(synced.error_code, 0);
        assert_eq!(synced.assignment, Bytes::from("p0,p1"));
        assert_eq!(state(&coordinator), Some(s("Stable")));
    }

    #[test]
    fn join_rejects_unknown_member_and_bad_timeouts() {
        let (_dir, coordinator) = coordinator();
        let now = Instant::now();
        let unknown = ready(coordinator.join(&request(&s("ghost"), &["range"]), 9, client(), now));
        assert_eq!(unknown.error_code, ResponseError::UnknownMemberId.code());
        let zero = request(&s(""), &["range"]).with_session_timeout_ms(0);
        assert_eq!(
            ready(coordinator.join(&zero, 9, client(), now)).error_code,
            ResponseError::InvalidSessionTimeout.code()
        );
        let unnamed = request(&s(""), &["range"]).with_group_id(GroupId(s("")));
        assert_eq!(
            ready(coordinator.join(&unnamed, 9, client(), now)).error_code,
            ResponseError::InvalidGroupId.code()
        );
    }

    #[test]
    fn join_before_v4_assigns_member_id_directly() {
        let (_dir, coordinator) = coordinator();
        let joined =
            ready(coordinator.join(&request(&s(""), &["range"]), 3, client(), Instant::now()));
        assert_eq!(joined.error_code, 0);
        assert!(joined.member_id.starts_with("client-"));
        assert_eq!(joined.leader, joined.member_id);
    }

    #[test]
    fn join_waits_for_every_member_and_followers_wait_for_assignment() {
        let (_dir, coordinator) = coordinator();
        let now = Instant::now();
        let (a, joined) = new_member(&coordinator, &["range"], now);
        ready(joined);
        ready(sync(&coordinator, &a, 1, &[(&a, "all")], now));

        let (b, b_joined) = new_member(&coordinator, &["range"], now);
        let mut b_joined = waiting(b_joined);
        assert!(
            b_joined.try_recv().is_err(),
            "join waits for the existing member"
        );
        assert_eq!(
            heartbeat(&coordinator, &a, 1, now),
            ResponseError::RebalanceInProgress.code()
        );

        let a_joined = ready(coordinator.join(&request(&a, &["range"]), 9, client(), now));
        let b_joined = b_joined.try_recv().unwrap();
        assert_eq!((a_joined.generation_id, b_joined.generation_id), (2, 2));
        assert_eq!(a_joined.leader, a, "the leader keeps leading");
        assert_eq!(a_joined.members.len(), 2);
        assert!(b_joined.members.is_empty(), "only the leader sees members");

        let mut b_synced = waiting(sync(&coordinator, &b, 2, &[], now));
        assert!(b_synced.try_recv().is_err());
        let a_synced = ready(sync(&coordinator, &a, 2, &[(&a, "p0"), (&b, "p1")], now));
        assert_eq!(a_synced.assignment, Bytes::from("p0"));
        assert_eq!(b_synced.try_recv().unwrap().assignment, Bytes::from("p1"));
        assert_eq!(heartbeat(&coordinator, &b, 2, now), 0);
        assert_eq!(
            ready(sync(&coordinator, &b, 2, &[], now)).assignment,
            Bytes::from("p1"),
            "a stable group answers sync from the stored assignment"
        );
    }

    #[test]
    fn heartbeat_checks_group_member_and_generation() {
        let (_dir, coordinator) = coordinator();
        let now = Instant::now();
        let unknown = ResponseError::UnknownMemberId.code();
        assert_eq!(heartbeat(&coordinator, &s("nobody"), 0, now), unknown);
        let (a, b) = stable_pair(&coordinator, now);
        assert_eq!(heartbeat(&coordinator, &a, 2, now), 0);
        assert_eq!(heartbeat(&coordinator, &b, 2, now), 0);
        assert_eq!(heartbeat(&coordinator, &s("nobody"), 2, now), unknown);
        assert_eq!(
            heartbeat(&coordinator, &a, 1, now),
            ResponseError::IllegalGeneration.code()
        );
    }

    #[test]
    fn session_expiry_removes_silent_member_and_rebalances() {
        let (_dir, coordinator) = coordinator();
        let start = Instant::now();
        let (a, b) = stable_pair(&coordinator, start);
        let later = start + SESSION / 2;
        assert_eq!(heartbeat(&coordinator, &a, 2, later), 0);

        coordinator.expire(start + SESSION - Duration::from_millis(1));
        assert_eq!(state(&coordinator), Some(s("Stable")), "nobody expired yet");
        let expired = start + SESSION + Duration::from_millis(1);
        coordinator.expire(expired);
        assert_eq!(state(&coordinator), Some(s("PreparingRebalance")));
        assert_eq!(
            heartbeat(&coordinator, &b, 2, expired),
            ResponseError::UnknownMemberId.code()
        );
        assert_eq!(
            heartbeat(&coordinator, &a, 2, expired),
            ResponseError::RebalanceInProgress.code()
        );
        let rejoined = ready(coordinator.join(&request(&a, &["range"]), 9, client(), expired));
        assert_eq!(rejoined.generation_id, 3);
        assert_eq!(rejoined.members.len(), 1);
    }

    #[test]
    fn member_waiting_on_join_is_not_session_expired() {
        let (_dir, coordinator) = coordinator();
        let now = Instant::now();
        let (a, b) = stable_pair(&coordinator, now);
        let (_, c_joined) = new_member(&coordinator, &["range"], now);
        let mut c_joined = waiting(c_joined);
        let mut a_joined = waiting(coordinator.join(&request(&a, &["range"]), 9, client(), now));
        for step in 1..=3 {
            let at = now + SESSION * step / 2;
            assert_eq!(
                heartbeat(&coordinator, &b, 2, at),
                ResponseError::RebalanceInProgress.code()
            );
            coordinator.expire(at);
        }
        assert!(a_joined.try_recv().is_err() && c_joined.try_recv().is_err());
        ready(coordinator.join(&request(&b, &["range"]), 9, client(), now + SESSION * 2));
        assert_eq!(a_joined.try_recv().unwrap().members.len(), 3);
        assert_eq!(c_joined.try_recv().unwrap().generation_id, 3);
    }

    #[test]
    fn rebalance_timeout_drops_members_that_do_not_rejoin() {
        let (_dir, coordinator) = coordinator();
        let now = Instant::now();
        let (a, b) = stable_pair(&coordinator, now);
        let (c, c_joined) = new_member(&coordinator, &["range"], now);
        let mut c_joined = waiting(c_joined);
        let mut a_joined = waiting(coordinator.join(&request(&a, &["range"]), 9, client(), now));
        let mut at = now;
        while at < now + REBALANCE {
            at += SESSION / 2;
            heartbeat(&coordinator, &b, 2, at);
            coordinator.expire(at);
        }
        let a_joined = a_joined.try_recv().unwrap();
        assert_eq!(a_joined.generation_id, 3);
        let ids: Vec<_> = a_joined
            .members
            .iter()
            .map(|m| m.member_id.clone())
            .collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&a) && ids.contains(&c) && !ids.contains(&b));
        assert_eq!(c_joined.try_recv().unwrap().generation_id, 3);
        assert_eq!(
            heartbeat(&coordinator, &b, 2, at),
            ResponseError::UnknownMemberId.code()
        );
    }

    #[test]
    fn leave_rebalances_and_last_leave_empties_group() {
        let (_dir, coordinator) = coordinator();
        let now = Instant::now();
        let (a, b) = stable_pair(&coordinator, now);
        assert_eq!(
            coordinator.leave(&s("g"), std::slice::from_ref(&a), now),
            [0]
        );
        assert_eq!(state(&coordinator), Some(s("PreparingRebalance")));
        let rejoined = ready(coordinator.join(&request(&b, &["range"]), 9, client(), now));
        assert_eq!(rejoined.generation_id, 3);
        assert_eq!(rejoined.leader, b, "leadership moves to a remaining member");

        assert_eq!(
            coordinator.leave(&s("g"), &[b.clone(), s("nobody")], now),
            [0, ResponseError::UnknownMemberId.code()]
        );
        assert_eq!(state(&coordinator), Some(s("Empty")));
        coordinator.expire(now);
        assert_eq!(
            state(&coordinator),
            None,
            "an empty group without offsets is forgotten"
        );
    }

    #[test]
    fn sync_checks_generation_and_rebalance_state() {
        let (_dir, coordinator) = coordinator();
        let now = Instant::now();
        let (a, b) = stable_pair(&coordinator, now);
        assert_eq!(
            ready(sync(&coordinator, &a, 1, &[], now)).error_code,
            ResponseError::IllegalGeneration.code()
        );
        let (c, _c_joined) = new_member(&coordinator, &["range"], now);
        assert_eq!(
            ready(sync(&coordinator, &b, 2, &[], now)).error_code,
            ResponseError::RebalanceInProgress.code()
        );
        let _a_joined = waiting(coordinator.join(&request(&a, &["range"]), 9, client(), now));
        ready(coordinator.join(&request(&b, &["range"]), 9, client(), now));
        let mut c_synced = waiting(sync(&coordinator, &c, 3, &[], now));
        coordinator.leave(&s("g"), &[b], now);
        assert_eq!(
            c_synced.try_recv().unwrap().error_code,
            ResponseError::RebalanceInProgress.code(),
            "a rebalance aborts syncs still waiting for the leader"
        );
    }

    #[test]
    fn protocol_selection_needs_a_protocol_every_member_supports() {
        let (_dir, coordinator) = coordinator();
        let now = Instant::now();
        let (a, joined) = new_member(&coordinator, &["range", "roundrobin"], now);
        ready(joined);
        ready(sync(&coordinator, &a, 1, &[], now));
        let refused = ready(coordinator.join(&request(&s(""), &["sticky"]), 9, client(), now));
        assert_eq!(
            refused.error_code,
            ResponseError::InconsistentGroupProtocol.code()
        );
        let other_type = request(&s(""), &["range"]).with_protocol_type(s("connect"));
        assert_eq!(
            ready(coordinator.join(&other_type, 9, client(), now)).error_code,
            ResponseError::InconsistentGroupProtocol.code()
        );

        let (_, b_joined) = new_member(&coordinator, &["roundrobin", "range"], now);
        let (_, c_joined) = new_member(&coordinator, &["sticky", "roundrobin"], now);
        let (mut b_joined, mut c_joined) = (waiting(b_joined), waiting(c_joined));
        let a_joined =
            ready(coordinator.join(&request(&a, &["range", "roundrobin"]), 9, client(), now));
        assert_eq!(a_joined.protocol_name, Some(s("roundrobin")));
        assert_eq!(a_joined.members.len(), 3);
        assert!(
            a_joined
                .members
                .iter()
                .all(|m| m.metadata == "roundrobin-metadata")
        );
        assert_eq!(
            b_joined.try_recv().unwrap().protocol_name,
            Some(s("roundrobin"))
        );
        assert_eq!(
            c_joined.try_recv().unwrap().protocol_name,
            Some(s("roundrobin"))
        );
    }

    #[test]
    fn pending_member_ids_delay_join_until_they_expire() {
        let (_dir, coordinator) = coordinator();
        let now = Instant::now();
        let first = ready(coordinator.join(&request(&s(""), &["range"]), 9, client(), now));
        let _second = ready(coordinator.join(&request(&s(""), &["range"]), 9, client(), now));
        let mut joined =
            waiting(coordinator.join(&request(&first.member_id, &["range"]), 9, client(), now));
        assert!(
            joined.try_recv().is_err(),
            "waits for the other handed-out member id"
        );
        coordinator.expire(now + SESSION + Duration::from_millis(1));
        assert_eq!(joined.try_recv().unwrap().members.len(), 1);
    }

    #[test]
    fn commit_checks_membership_generation_and_state() {
        let (_dir, coordinator) = coordinator();
        let now = Instant::now();
        let g = s("g");
        assert_eq!(
            coordinator.check_commit(&g, &s(""), -1),
            Ok(()),
            "outside commit to an idle group"
        );
        let (a, b) = stable_pair(&coordinator, now);
        let unknown = Err(ResponseError::UnknownMemberId.code());
        assert_eq!(coordinator.check_commit(&g, &s(""), -1), unknown);
        assert_eq!(coordinator.check_commit(&g, &s("nobody"), 2), unknown);
        assert_eq!(
            coordinator.check_commit(&g, &a, 1),
            Err(ResponseError::IllegalGeneration.code())
        );
        assert_eq!(coordinator.check_commit(&g, &a, 2), Ok(()));
        let (_c, _c_joined) = new_member(&coordinator, &["range"], now);
        assert_eq!(
            coordinator.check_commit(&g, &b, 2),
            Ok(()),
            "members may still commit while the group prepares a rebalance"
        );
        let _a_joined = waiting(coordinator.join(&request(&a, &["range"]), 9, client(), now));
        ready(coordinator.join(&request(&b, &["range"]), 9, client(), now));
        assert_eq!(
            coordinator.check_commit(&g, &b, 3),
            Err(ResponseError::RebalanceInProgress.code())
        );
        assert_eq!(
            coordinator.check_commit(&s(""), &s(""), -1),
            Err(ResponseError::InvalidGroupId.code())
        );
    }

    #[tokio::test]
    async fn committed_offsets_survive_reopen() {
        let (dir, coordinator) = coordinator();
        let group = s("weird/group id\twith ü");
        let committed = |offset, metadata: Option<&str>| Committed {
            offset,
            leader_epoch: 4,
            metadata: metadata.map(s),
        };
        let first = vec![
            ((s("orders"), 0), committed(10, Some(""))),
            ((s("orders"), 1), committed(20, None)),
        ];
        assert_eq!(coordinator.commit(&group, &s(""), -1, first).await, 0);
        let second = vec![((s("orders"), 1), committed(25, Some("tab\there\nnewline")))];
        assert_eq!(coordinator.commit(&group, &s(""), -1, second).await, 0);
        let expected = coordinator.committed(&group).await;
        assert_eq!(expected.len(), 2);
        assert_eq!(
            expected[&(s("orders"), 1)],
            committed(25, Some("tab\there\nnewline"))
        );
        drop(coordinator);

        std::fs::write(dir.path().join("groups").join("abcd.tmp"), "torn").unwrap();
        let reopened = Coordinator::open(dir.path().join("groups")).unwrap();
        assert_eq!(reopened.committed(&group).await, expected);
        assert!(reopened.committed(&s("other")).await.is_empty());
        assert_eq!(
            reopened.describe(&group).map(|g| g.group_state),
            Some(s("Empty")),
            "a group with offsets but no members is described as empty"
        );
        assert_eq!(reopened.list().len(), 1);
        assert!(!dir.path().join("groups").join("abcd.tmp").exists());
    }

    #[test]
    fn hex_round_trips_and_rejects_garbage() {
        assert_eq!(hex(b"g-1"), "672d31");
        assert_eq!(unhex("672d31"), Some(b"g-1".to_vec()));
        assert_eq!(unhex("672"), None);
        assert_eq!(unhex("+f"), None);
        assert_eq!(unhex("zz"), None);
    }
}
