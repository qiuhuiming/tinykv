// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// random election interval
	randomElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	peers []uint64

	logger *logger
}

const invalidId uint64 = 0

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	nPeers := len(c.peers)
	peers := make([]uint64, nPeers)
	copy(peers, c.peers)

	r := &Raft{
		id:               c.ID,
		Term:             0,
		Vote:             invalidId,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		Lead:             invalidId,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: 0,
		logger:           newLogger(c.ID, readLevelFromEnv()),
		peers:            peers,
	}
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower, StateCandidate:
		r.tickElection()
	case StateLeader:
		r.tickHeartbeat()
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower
	r.logger.Infof("became follower at term(%d)", term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.logger.Fatalf("invalid transition [leader -> candidate]")
	}
	// Increase term and vote for self.
	term := r.Term + 1
	r.reset(term)
	r.Vote = r.id
	r.votes[r.id] = true
	r.State = StateCandidate
	r.logger.Infof("become candidate at term(%d)", term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State == StateFollower {
		r.logger.Fatalf("invalid transition [follower -> candidate]")
	}

	term := r.Term
	r.reset(term)
	r.Lead = r.id
	r.State = StateLeader

	// TODO: update nextIndex, matchIndex 2ab

	r.logger.Infof("became leader at term(%d)", term)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	switch {
	case m.Term == 0:
		// Local message
	case m.Term < r.Term:
		// Just ignore the msg because we don't implement pro_vote and check_quorom.
		r.logger.Infof("receive msg from old term: type = %s, from = %d", m.MsgType.String(), m.From)
		return nil
	case m.Term > r.Term:
		// Find newer term, so the state should convert to follower.
		// If the msg type is Append, Heartbeat, or Snapshot, the sender must be the leader in the new term.
		// Else we don't know who is the leader, so we set `lead` to invalidId.
		switch m.MsgType {
		case pb.MessageType_MsgAppend, pb.MessageType_MsgHeartbeat, pb.MessageType_MsgSnapshot:
			r.becomeFollower(m.Term, m.From)
		default:
			r.becomeFollower(m.Term, invalidId)
		}
	case m.Term == r.Term:
		// If the raft instance is a candidate, it may **receive message from the leader at the same term**.
		// We should consider converting the state to follower.
		if r.State == StateCandidate {
			switch m.MsgType {
			case pb.MessageType_MsgAppend, pb.MessageType_MsgHeartbeat, pb.MessageType_MsgSnapshot:
				r.becomeFollower(m.Term, m.From)
			}
		}
	}

	// We handle the message according to the type.
	// Here assert m.Term == r.Term for remote message.
	switch m.MsgType {
	// Local
	case pb.MessageType_MsgHup:
		r.handleHup(m)
	case pb.MessageType_MsgBeat:
		r.handleBeat(m)
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)

	// RPC Request
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)

	// RPC Response
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)

	// TODO(2ab, 2ac): more types
	default:
		r.logger.Warnf("unexpected message type: %s", m.MsgType.String())
		return ErrorUnexpectedMsgType
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.State != StateFollower {
		r.logger.Warnf("non-follower: receive heartbeat, from(%d), term(%d)")
		return
	}
	r.Lead = m.From
	r.electionElapsed = 0
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// handleRequestVote handle RequestVote RPC	 request
func (r *Raft) handleRequestVote(m pb.Message) {
	// Your Code Here (2A)
	canVote := m.From == r.Vote || (r.Vote == invalidId && r.Lead == invalidId)
	// Cast
	if canVote && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
		r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
			r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)
		r.send(pb.Message{To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: false})
		r.electionElapsed = 0
		r.Vote = m.From
		return
	}

	// Reject
	r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
		r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)
	r.send(pb.Message{To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
	return
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	// Assert m.Term == r.Term (in r.Step(m))
	if r.State != StateCandidate {
		r.logger.Debugf("none-candidate: receive vote response: from(%d), term(%d), rejected(%s)", m.From, m.Term, m.Reject)
		return
	}

	granted, rejected, result := r.poll(m.From, pb.MessageType_MsgRequestVote, !m.Reject)
	r.logger.Infof("has received granted(%d), rejected(%d)", granted, rejected)
	if result == VoteWon {
		r.logger.Infof("won the election at term(%d)", m.Term)
		r.becomeLeader()
		r.broadcastHeartbeat()
		// TODO(2ab): append a noop log entry
	} else if result == VoteLost {
		r.becomeFollower(r.Term, invalidId)
	}
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	// Do nothing now.
	// TODO(2ab):
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	// TODO(2ab):
}

func (r *Raft) handleHup(m pb.Message) {
	if r.State == StateLeader {
		r.logger.Debugf("ignore hup: already leader")
		return
	}

	r.logger.Infof("start a new election at term(%d)", r.Term)
	r.campaign()
}

func (r *Raft) handleBeat(m pb.Message) {
	if r.State != StateLeader {
		r.logger.Warnf("none-leader: received Beat")
		return
	}

	r.broadcastHeartbeat()
}

func (r *Raft) handlePropose(m pb.Message) {
	// TODO: 2ab
}

func (r *Raft) broadcastHeartbeat() {
	for _, id := range r.peers {
		if id == r.id {
			continue
		}

		// TODO(2ab): change commitId
		commit := r.RaftLog.committed
		r.send(pb.Message{
			To:      id,
			MsgType: pb.MessageType_MsgHeartbeat,
			Commit:  commit,
		})
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) tickHeartbeat() {
	r.electionElapsed++
	r.heartbeatElapsed++

	if r.State != StateLeader {
		return
	}
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.logger.Debugf("tickHeartbeat: timeout")
		r.heartbeatElapsed = 0
		msg := pb.Message{
			MsgType: pb.MessageType_MsgBeat,
			Term:    0,
			From:    r.id,
			To:      invalidId,
		}
		err := r.Step(msg)
		if err != nil {
			r.logger.Errorf("tickHeartbeat: error stepping Beat: %s", err.Error())
		}
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++

	if r.electionElapsed >= r.randomElectionTimeout {
		r.logger.Infof("tickElection: timeout")
		r.electionElapsed = 0
		msg := pb.Message{MsgType: pb.MessageType_MsgHup, Term: 0, From: r.id, To: invalidId}
		err := r.Step(msg)
		if err != nil {
			r.logger.Errorf("tickElection: error stepping Hup: %s", err.Error())
		}
	}
}

func (r *Raft) reset(term uint64) {
	if term != r.Term {
		r.Term = term
	}
	r.Lead = invalidId
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetElectionTimeout()
	r.resetVotes()

	// TODO(2ab): reset logState
}

func (r *Raft) resetVotes() {
	r.Vote = invalidId
	r.votes = make(map[uint64]bool)
}

// resetElectionTimeout resets the electionTimeout to a random number.
func (r *Raft) resetElectionTimeout() {
	r.randomElectionTimeout = r.electionTimeout + int(rand.Int31n(int32(r.electionTimeout)))
}

func (r *Raft) send(m pb.Message) {
	if m.From == None {
		m.From = r.id
	}
	switch m.MsgType {
	case pb.MessageType_MsgRequestVote, pb.MessageType_MsgRequestVoteResponse:
		if m.Term == 0 {
			r.logger.Fatalf("term should be set when sending %s", m.MsgType.String())
		}
	default:
		if m.Term != 0 {
			r.logger.Fatalf("term should not be set when sending %s", m.MsgType.String())
		}
		if m.MsgType != pb.MessageType_MsgPropose {
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) campaign() {
	r.becomeCandidate()

	term := r.Term
	_, _, res := r.poll(r.id, pb.MessageType_MsgRequestVote, true)
	if res == VoteWon {
		r.becomeLeader()
		return
	}

	for _, id := range r.peers {
		if id == r.id {
			continue
		}
		r.logger.Infof("send %s request to peer(%d) at term(%d)", pb.MessageType_MsgRequestVote.String(), id, term)
		r.send(pb.Message{
			To:      id,
			Term:    term,
			MsgType: pb.MessageType_MsgRequestVote,
			Index:   r.RaftLog.LastIndex(),
			LogTerm: r.RaftLog.LastTerm(),
		})
	}
}

type VoteResult uint8

const (
	// VotePending indicates that the decision of the vote depends on future
	// votes, i.e. neither "yes" or "no" has reached quorum yet.
	VotePending VoteResult = 1 + iota
	// VoteLost indicates that the quorum has voted "no".
	VoteLost
	// VoteWon indicates that the quorum has voted "yes".
	VoteWon
)

func (r *Raft) poll(id uint64, voteType pb.MessageType, v bool) (granted uint64, rejected uint64, result VoteResult) {
	if v {
		r.logger.Infof("received granted vote from peer(%d) at term(%d)", id, r.Term)
	} else {
		r.logger.Infof("received rejected vote from peer(%d) at term(%d)", id, r.Term)
	}

	// Record vote
	_, ok := r.votes[id]
	if !ok {
		r.votes[id] = v
	}

	return r.TallyVotes()
}

func (r *Raft) TallyVotes() (granted uint64, rejected uint64, result VoteResult) {
	var missing uint64 = 0
	for _, id := range r.peers {
		v, ok := r.votes[id]
		if !ok {
			missing++
		}
		if v {
			granted++
		} else {
			rejected++
		}
	}

	majorityNum := uint64(len(r.peers)/2 + 1)
	if granted >= majorityNum {
		result = VoteWon
	} else if rejected+missing >= majorityNum {
		result = VotePending
	} else {
		result = VoteLost
	}

	return
}
