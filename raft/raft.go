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
	"fmt"
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

	logger *logger
}

const invalidId uint64 = 0

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	prs := make(map[uint64]*Progress)
	for _, id := range c.peers {
		prs[id] = new(Progress)
	}

	hardState, _, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	r := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              prs,
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		Lead:             invalidId,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: 0,
		logger:           newLogger(fmt.Sprintf("Raft(%d)", c.ID), readLevelFromEnv()),
	}
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if to == r.id {
		r.logger.Warnf("sendAppend to self")
		return false
	}

	nextIndex := r.Prs[to].Next
	ents, err := r.RaftLog.maybeEntries(nextIndex, r.RaftLog.LastIndex())
	if err != nil && err != ErrorEmptyEntries {
		r.logger.Errorf("sendAppend: failed: %s", err.Error())
		return false
	}

	Ents := make([]*pb.Entry, len(ents))
	for i := range Ents {
		Ents[i] = &ents[i]
	}

	// TODO(2ab, 2ac): consider snapshot
	prevLogIndex := nextIndex - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		prevLogTerm = 0
	}

	m := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: Ents,
		Commit:  r.RaftLog.committed,
	}
	r.send(m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	commit := min(r.RaftLog.committed, r.Prs[to].Match)
	r.send(pb.Message{
		To:      to,
		MsgType: pb.MessageType_MsgHeartbeat,
		Commit:  commit,
	})
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
		r.logger.Panicf("invalid transition [leader -> candidate]")
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
		r.logger.Panicf("invalid transition [follower -> candidate]")
	}

	term := r.Term
	r.reset(term)
	r.Lead = r.id
	r.State = StateLeader

	if !r.appendEntries([]*pb.Entry{&pb.Entry{Data: nil}}) {
		r.logger.Panicf("cannot append noop entry at term(%d)", r.Term)
		return
	}

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
	var err error
	switch m.MsgType {
	// Local
	case pb.MessageType_MsgHup:
		r.handleHup(m)
	case pb.MessageType_MsgBeat:
		r.handleBeat(m)
	case pb.MessageType_MsgPropose:
		err = r.handlePropose(m)

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
	return err
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if r.State != StateFollower {
		r.logger.Debugf("ignore appendEntries: state: %s, want: %s", r.State, StateFollower.String())
		return
	}

	r.Lead = m.From
	r.electionElapsed = 0

	if r.RaftLog.matchTerm(m.Index, m.LogTerm) {
		// Matched (prevLogIndex, prevLogTerm)
		li, err := r.RaftLog.MustAppend(m.Index, m.Entries)
		if err != nil {
			r.logger.Panicf("handleAppendEntries: %s", err.Error())
			return
		}

		r.RaftLog.commitTo(min(li, m.Commit))

		rm := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			To:      m.From,
			Index:   li,
			Reject:  false,
		}
		r.send(rm)
		return
	}

	// TODO(2ab): optimize by hinting conflict ones.
	rm := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		Index:   m.Index,
		Reject:  true,
	}
	r.send(rm)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.State != StateFollower {
		r.logger.Debugf("non-follower: receive heartbeat, from(%d), term(%d)", m.From, r.Term)
		return
	}
	r.Lead = m.From
	r.electionElapsed = 0
	r.RaftLog.commitTo(m.Commit)

	rm := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.From,
	}
	r.send(rm)
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
		r.logger.Debugf("none-candidate: receive vote response: from(%d), term(%d), rejected(%t)", m.From, m.Term, m.Reject)
		return
	}

	granted, rejected, result := r.poll(m.From, pb.MessageType_MsgRequestVote, !m.Reject)
	r.logger.Infof("has received granted(%d), rejected(%d)", granted, rejected)
	if result == VoteWon {
		r.logger.Infof("won the election at term(%d)", m.Term)
		r.becomeLeader()
		r.broadcastAppend()
	} else if result == VoteLost {
		r.becomeFollower(r.Term, invalidId)
	}
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if r.State != StateLeader {
		r.logger.Debugf("ignore AppResp: not the leader")
		return
	}

	if m.Reject {
		// Decrease nextIndex[m.from]
		r.Prs[m.From].Next--
		r.sendAppend(m.From)
		return
	}

	if r.maybeUpdateProgress(m.From, m.Index) {
		if r.maybeCommit() {
			r.broadcastAppend()
		}
	}

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

var (
	ErrorProposeDropped = errors.New("raft proposal dropped")
)

func (r *Raft) handlePropose(m pb.Message) error {
	if r.State != StateLeader {
		r.logger.Debugf("ignore propose: state(%s)", r.State.String())
		return ErrorProposeDropped
	}

	if len(m.Entries) == 0 {
		r.logger.Panicf("empty entries in propose")
		return ErrProposalDropped
	}

	if !r.appendEntries(m.Entries) {
		r.logger.Errorf("error: appendEntries")
		return ErrorProposeDropped
	}

	r.broadcastAppend()
	return nil
}

func (r *Raft) broadcastHeartbeat() {
	for to, _ := range r.Prs {
		if to == r.id {
			continue
		}
		r.sendHeartbeat(to)
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
	for id, pr := range r.Prs {
		*pr = Progress{
			Match: 0,
			Next:  r.RaftLog.LastIndex() + 1,
		}
		if id == r.id {
			pr.Match = r.RaftLog.LastIndex()
		}
	}
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
			r.logger.Panicf("term should be set when sending %s", m.MsgType.String())
		}
	default:
		if m.Term != 0 {
			r.logger.Panicf("term should not be set when sending %s", m.MsgType.String())
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

	for id := range r.Prs {
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
	for id := range r.Prs {
		v, ok := r.votes[id]
		if !ok {
			missing++
			continue
		}
		if v {
			granted++
		} else {
			rejected++
		}
	}

	majorityNum := uint64(len(r.Prs)/2 + 1)
	if granted >= majorityNum {
		result = VoteWon
	} else if rejected >= majorityNum {
		result = VoteLost
	} else {
		result = VotePending
	}

	return
}

func (r *Raft) appendEntries(entries []*pb.Entry) (accepted bool) {
	lastIndex := r.RaftLog.LastIndex()
	for i, ent := range entries {
		ent.Term = r.Term
		ent.Index = lastIndex + uint64(i) + 1
	}

	lastIndex = r.RaftLog.append(entries)
	r.maybeUpdateProgress(r.id, lastIndex)

	r.maybeCommit()
	return true
}

func (r *Raft) broadcastAppend() {
	for to, _ := range r.Prs {
		if to == r.id {
			continue
		}
		r.sendAppend(to)
	}
}

func (r *Raft) maybeUpdateProgress(id uint64, index uint64) (updated bool) {
	pr, ok := r.Prs[id]
	if !ok {
		r.logger.Panicf("progress not found")
		return false
	}

	if pr.Match < index {
		pr.Match = index
		updated = true
	}
	pr.Next = max(pr.Next, index+1)
	return
}

func (r *Raft) maybeCommit() bool {
	ci := r.newCommitIndex()
	return r.RaftLog.maybeCommit(ci)
}

// newCommitIndex is called by the leader to find the newCommitIndex(N)
// having: a majority of matchIndex[i] >= N and log[N].Term == r.Term
// set r.commitIndex = N
// refer: figure 2 of the raft-extended paper
func (r *Raft) newCommitIndex() (newCommitIndex uint64) {
	lo := r.RaftLog.committed + 1
	hi := r.RaftLog.LastIndex()

	maj := len(r.Prs)/2 + 1

	// TODO: optimize using binary-search
	for N := lo; N <= hi; N++ {
		term := r.RaftLog.zeroTermOnErrCompacted(r.RaftLog.Term(N))
		if term < r.Term {
			continue
		}
		if term > r.Term {
			r.logger.Errorf("found higher term entry in the leader")
			return
		}

		matchedCount := 0
		for _, pr := range r.Prs {
			if pr.Match >= N {
				matchedCount++
			}
		}
		if matchedCount >= maj {
			newCommitIndex = N
		} else {
			// the following entries cannot be committed now, so we can break the loop.
			break
		}
	}
	return
}

func (r *Raft) Msgs() []pb.Message {
	return r.msgs
}

func (r *Raft) SoftState() SoftState {
	return SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) HardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) advance(rd Ready) {
	if na := rd.newApplied(); na > 0 {
		r.RaftLog.appliedTo(na)
	}

	if n := len(rd.Entries); n > 0 {
		e := rd.Entries[n-1]
		r.RaftLog.stableTo(e.Index, e.Term)
	}

}

func (r *Raft) clearMsgs() {
	r.msgs = make([]pb.Message, 0)
}
