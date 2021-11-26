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
	"fmt"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pkg/errors"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	logger *logger
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	fi, _ := storage.FirstIndex()
	li, _ := storage.LastIndex()
	entries, _ := storage.Entries(fi, li+1)
	hardState, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}

	return &RaftLog{
		storage:         storage,
		committed:       hardState.Commit,
		applied:         0,
		stabled:         li,
		entries:         entries,
		pendingSnapshot: nil,
		logger:          newLogger("", readLevelFromEnv()),
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return nil
}

// entriesBetween returns the entries whose index in [start, end].
func (l *RaftLog) entriesBetween(start uint64, end uint64) (ents []pb.Entry, err error) {
	return nil, nil
}

var ErrorEmptyEntries = errors.New("empty entries")

// maybeEntries returns the entries whose index in [start, end].
func (l *RaftLog) maybeEntries(start uint64, end uint64) (ents []pb.Entry, err error) {
	if end < start {
		err = ErrorEmptyEntries
		return
	}
	if start < l.FirstIndex() {
		err = errors.New("the start log has been compacted")
	}
	if end > l.LastIndex() {
		err = errors.New("end > lastIndex")
		return
	}

	firstIndex := l.FirstIndex()
	start -= firstIndex
	end -= firstIndex

	ents = make([]pb.Entry, end-start+1)
	srcEnd := min(uint64(len(l.entries)), end+1)
	src := l.entries[start:srcEnd]
	copy(ents, src)
	return
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	ents, err := l.maybeEntries(l.stabled+1, l.LastIndex())
	if err != nil {
		return []pb.Entry{}
	}

	return ents
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	ents, err := l.maybeEntries(l.applied+1, l.committed)
	if err != nil {
		return nil
	}

	return ents
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return 0
	}

	return l.entries[len(l.entries)-1].Index
}

// FirstIndex returns the first index of the log entries
func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[0].Index
}

// maybeEntry returns the entry whose index == `i`
// return error if `i` is out of range
func (l *RaftLog) maybeEntry(i uint64) (ent pb.Entry, err error) {
	firstIndex := l.FirstIndex()
	i -= firstIndex

	if i < 0 {
		return ent, ErrCompacted
	}
	if i >= uint64(len(l.entries)) {
		return ent, ErrUnavailable
	}

	ent = l.entries[i]
	return
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// TODO: read from storage
	ent, err := l.maybeEntry(i)
	switch err {
	case nil:
		return ent.Term, nil
	case ErrCompacted, ErrUnavailable:
		return 0, nil
	default:
		panic(err)
	}
}

func (l *RaftLog) LastTerm() uint64 {
	term, _ := l.Term(l.LastIndex())
	return term
}

func (l *RaftLog) isUpToDate(index uint64, term uint64) bool {
	return term > l.LastTerm() || (term == l.LastTerm() && index >= l.LastIndex())
}

func (l *RaftLog) append(entries []*pb.Entry) (lastIndex uint64) {
	if len(entries) == 0 {
		return l.LastIndex()
	}

	if entries[0].Index <= l.committed {
		l.logger.Panicf("append log whose index <= committedIndex")
		return 0
	}

	Entries := make([]pb.Entry, len(entries))
	for i, ent := range entries {
		Entries[i] = *ent
	}
	l.truncateAndAppend(Entries)
	return l.LastIndex()
}

func (l *RaftLog) maybeCommit(ci uint64) bool {
	// TODO: maybe we need to check if it is been compacted.
	if ci <= l.committed {
		return false
	}

	l.commitTo(ci)
	return true
}

func (l *RaftLog) commitTo(ci uint64) {
	if ci <= l.committed {
		return
	}

	if li := l.LastIndex(); ci > li {
		l.logger.Panicf("commitTo: newCommitIndex out of range, ci(%d), li(%d)", ci, li)
		return
	}
	l.committed = ci
}

func (l *RaftLog) matchTerm(index uint64, term uint64) bool {
	t, err := l.Term(index)
	if err != nil {
		return false
	}
	return t == term
}

func (l *RaftLog) MustAppend(prevIndex uint64, entries []*pb.Entry) (lastNewIndex uint64, err error) {
	lastNewIndex = prevIndex + uint64(len(entries))
	if len(entries) == 0 {
		return
	}

	conflictIndex := l.findConflict(entries)
	switch {
	case conflictIndex == 0:
		// Do nothing
	case conflictIndex <= l.committed:
		return 0, errors.New(fmt.Sprintf("entry %d conflict with committed entry [committed(%d)]", conflictIndex, l.committed))
	default:
		offset := entries[0].Index
		l.append(entries[conflictIndex-offset:])
	}
	return
}

// findConflict finds the conflict entries between `l.entries` and `entries`
// The method must be called when handling appendEntries RPC and matchTerm(prevLogIndex, prevLogTerm).
// The method return the first entry index at which there is a conflict entry.
// If all entries are matched, return 0
func (l *RaftLog) findConflict(entries []*pb.Entry) (conflictIndex uint64) {
	// Compare `entries` and `l.entries`
	// TODO(2ab): Use binary-search to optimize
	for _, ent := range entries {
		if !l.matchTerm(ent.Index, ent.Term) {
			if ent.Index <= l.LastIndex() {
				l.logger.Infof("found conflict at index(%d) [exist(%d), want(%d)]", ent.Index, l.zeroTermOnErrCompacted(l.Term(ent.Index)), ent.Term)
			}
			return ent.Index
		}
		return
	}

	return 0
}

func (l *RaftLog) truncateAndAppend(entries []pb.Entry) {
	if len(entries) == 0 {
		return
	}
	entriesFirstIndex := entries[0].Index
	currentFirstIndex := l.FirstIndex()
	switch {
	case entriesFirstIndex == currentFirstIndex+uint64(len(l.entries)):
		l.entries = append(l.entries, entries...)
	case entriesFirstIndex <= currentFirstIndex:
		l.logger.Infof("replace the unstable entries from index %d", entriesFirstIndex)
		l.entries = entries
		l.stableTo(entriesFirstIndex-1, entries[0].Term)
	default:
		l.logger.Infof("truncate the unstable entries before index %d", entriesFirstIndex)
		end := min(entriesFirstIndex-currentFirstIndex, uint64(len(l.entries)))
		l.entries = l.entries[:end]
		l.entries = append(l.entries, entries...)
		l.stableTo(entriesFirstIndex-1, entries[0].Term)
	}
}

func (l *RaftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	l.logger.Panicf("unexpected error (%v)", err)
	return 0
}

func (l *RaftLog) stableTo(index, term uint64) {
	// TODO:
	l.stabled = index
}

// maybeNextEntries returns the entries
func (l *RaftLog) maybeNextEntries() []pb.Entry {
	entries, err := l.maybeEntries(max(l.applied+1, l.FirstIndex()), l.committed)
	if err != nil {
		entries = []pb.Entry{}
	}
	return entries
}

func (l *RaftLog) maybeUnstable() []pb.Entry {
	entries, err := l.maybeEntries(l.stabled+1, l.LastIndex())
	if err != nil {
		entries = []pb.Entry{}
	}
	return entries
}

func (l *RaftLog) HasNext() bool {
	return l.committed > l.applied && l.committed >= l.FirstIndex()
}

func (l *RaftLog) HasUnstable() bool {
	return l.LastIndex() > l.stabled
}

func (l *RaftLog) appliedTo(applied uint64) {
	if applied == 0 {
		return
	}
	if l.committed < applied || applied < l.applied {
		l.logger.Panicf("appliedTo: out of range: [prevApplied(%d), committed](%d)", l.applied, l.committed)
	}
	l.applied = applied
}
