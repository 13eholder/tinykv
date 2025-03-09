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

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

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
	unstabled uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	raftLog := &RaftLog{
		storage: storage,
		entries: make([]pb.Entry, 0),
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	// storage的FirstIndex返回的是实际的FirstIndex + 1
	raftLog.committed = firstIndex - 1
	raftLog.applied = firstIndex - 1
	raftLog.stabled = lastIndex
	// raftLog.Entry的起始Index
	raftLog.unstabled = lastIndex + 1
	return raftLog
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
	if len(l.entries) == 0 {
		return nil
	}
	return l.entries[0 : l.entries[len(l.entries)-1].Index-l.stabled]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	end := l.entries[len(l.entries)-1].Index - l.stabled
	return l.entries[0:end]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return nil
	}
	return l.entries[l.applied-l.stabled : l.committed-l.stabled]
}

// return l.entries[begin,end)
func (l *RaftLog) Entries(begin, end uint64) []*pb.Entry {
	if begin > l.LastIndex() || end <= l.entries[0].Index {
		return nil
	}
	ents := make([]*pb.Entry, 0)
	for _, ent := range l.entries {
		if begin <= ent.Index && ent.Index < end {
			ents = append(ents, &ent)
		}
	}
	return ents
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	// unstable -> snapshot -> storage
	if len(l.entries) != 0 {
		// DEBUG.Printf("RaftLog get lastIndex %d from unstable entries\n", l.entries[len(l.entries)-1].Index)
		return l.entries[len(l.entries)-1].Index
	}
	if l.pendingSnapshot != nil {
		// DEBUG.Printf("RaftLog get lastIndex %d from pending snapshot\n", l.pendingSnapshot.Metadata.Index)
		return l.pendingSnapshot.Metadata.Index
	}
	lastIndex, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	// DEBUG.Printf("RaftLog get lastIndex %d from storage\n", lastIndex)
	return lastIndex
}

func (l *RaftLog) LastTerm() uint64 {
	term, err := l.Term(l.LastIndex())
	if err != nil {
		panic(err)
	}
	return term
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// unstable -> storage
	for _, entry := range l.entries {
		if entry.Index == i {
			return entry.Term, nil
		}
	}
	term, err := l.storage.Term(i)
	if err == nil {
		return term, nil
	}

	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}

	panic(err)
}

func (l *RaftLog) Append(ents []pb.Entry) {
	// 在RaftLog中找到第一个和ents[0] Index相等的Entry
	index := 0
	for _, entry := range l.entries {
		if entry.Index == ents[0].Index {
			break
		}
		index++
	}
	if index == 0 {
		l.entries = make([]pb.Entry, 0)
	} else {
		l.entries = l.entries[0:index]
	}
	l.entries = append(l.entries, ents...)
}

func (l *RaftLog) Exist(index uint64) bool {
	if index < l.entries[0].Index || index > l.entries[len(l.entries)-1].Index {
		return false
	}
	// TODO(ZMY):使用二分查找
	for _, entry := range l.entries {
		if entry.Index == index {
			return true
		}
	}
	return false
}

func (l *RaftLog) CommitTo(i uint64) {
	l.committed = i
}

func (l *RaftLog) ApplieTo(i uint64) {
	l.applied = i
}
