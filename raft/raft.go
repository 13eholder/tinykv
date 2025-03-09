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
	"log"
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
	// Customed

	// [electionTimeout,2*electionTimeout)
	randomElectionTimeout int
	// 成为Leader所需的多数人数
	quorum int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hs, _, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	raftLog := newLog(c.Storage)
	prs := make(map[uint64]*Progress, len(c.peers))
	// 在选举出Leader后重新初始化
	for _, peer := range c.peers {
		prs[peer] = &Progress{}
	}

	raft := &Raft{
		id:               c.ID,
		Term:             0,
		Vote:             None,
		RaftLog:          raftLog,
		Prs:              prs,
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		quorum:           len(prs)/2 + 1,
	}
	raft.setRandomElectionTimeout()
	// raft可能是一个新节点,也可能是一个重新启动的节点
	if !IsEmptyHardState(hs) {
		raft.applyHardState(hs)
	}
	if c.Applied > 0 {
		raft.RaftLog.ApplieTo(c.Applied)
	}
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	// Next指向下一个待添加的日志项,复制日志时需要有相同的匹配项才可以
	prevLogIndex := r.Prs[to].Next - 1
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		log.Fatalf("sendAppend failed to get prevLogTerm when prevLogIndex == %d\n", prevLogIndex)
		panic(err)
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		LogTerm: prevLogTerm,
		Index:   prevLogIndex,
		Entries: r.RaftLog.Entries(prevLogIndex, r.RaftLog.LastIndex()+1),
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term})
}

func (r *Raft) sendRequestVote(to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		LogTerm: r.RaftLog.LastTerm(),
	})
}

func (r *Raft) bcastHeartBeat() {
	for to := range r.Prs {
		if to != r.id {
			r.sendHeartbeat(to)
		}
	}
}

func (r *Raft) bcastRequestVote() {
	for to := range r.Prs {
		if to != r.id {
			r.sendRequestVote(to)
		}
	}
}

func (r *Raft) bcastAppend() {
	for to := range r.Prs {
		if to != r.id {
			r.sendAppend(to)
		}
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.isLeader() {
		r.tickHeartbeat()
	} else {
		r.tickElection()
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed == r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
	}
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed == r.randomElectionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.Lead = lead
	r.Vote = None
	r.electionElapsed = 0
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term++
	r.Lead = None
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.Vote = r.id
	r.setRandomElectionTimeout()
	r.State = StateCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.Vote = None
	r.votes = make(map[uint64]bool)
	r.Lead = r.id
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.State = StateLeader
	for id := range r.Prs {
		if id == r.id {
			r.Prs[id] = &Progress{}
		} else {
			r.Prs[id] = &Progress{
				Match: 0,
				Next:  r.RaftLog.LastIndex() + 1,
			}
		}
	}
	// append noop and then bcastappend
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{
			EntryType: pb.EntryType_EntryNormal,
			Term:      r.Term,
			Index:     r.RaftLog.LastIndex() + 1,
		}},
	})
}

func (r *Raft) setRandomElectionTimeout() {
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

func (r *Raft) applyHardState(hs pb.HardState) {
	r.Term = hs.Term
	r.Vote = hs.Vote
	r.RaftLog.committed = hs.Commit
}

func (r *Raft) isLeader() bool {
	return r.State == StateLeader
}

func (r *Raft) isCandidate() bool {
	return r.State == StateCandidate
}

func (r *Raft) isFollower() bool {
	return r.State == StateFollower
}

// 成为候选人,投票给自己并请求其他节点投票
func (r *Raft) campaign() {
	r.becomeCandidate()
	r.bcastRequestVote()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	panic(fmt.Errorf("unknown raft state %d", r.State))
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartBeat()
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
		if r.winElection() {
			r.becomeLeader()
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
		if r.winElection() {
			r.becomeLeader()
		}
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.campaign()
		if r.winElection() {
			r.becomeLeader()
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	}

	return nil
}

func (r *Raft) winElection() bool {
	voteNum := 0
	for _, res := range r.votes {
		if res {
			voteNum++
		}
	}
	return voteNum >= r.quorum
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if !r.isCandidate() && m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	if r.isCandidate() && m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
	}

	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	}
	if m.Term < r.Term {
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}
	// 如果没有日志,肯定匹配成功
	matchPrevLogIndex := len(r.RaftLog.entries) == 0
	for _, entry := range r.RaftLog.entries {
		if entry.Index == m.Index {
			matchPrevLogIndex = true
			break
		}
	}
	if !matchPrevLogIndex {
		msg.Reject = true
		r.msgs = append(r.msgs, msg)
		return
	}
	if m.Commit > r.RaftLog.committed {
		lastNewIndex := m.Commit
		if !r.RaftLog.Exist(m.Entries[len(m.Entries)-1].Index) {
			lastNewIndex = m.Entries[len(m.Entries)-1].Index
		}
		r.RaftLog.CommitTo(min(m.Commit, lastNewIndex))
	}
	r.RaftLog.Append(toEntrySlice(m.Entries))
	msg.Index = r.RaftLog.LastIndex()
	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	//
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term, // 为了让candidate更新自己,考虑一个candidate掉线很久后重连的情况
		Reject:  false,
	}
	// 日志是否至少跟receiver一样新
	if m.Term < r.Term || (r.Vote != None && r.Vote != m.From) ||
		!isUpToDate(m.LogTerm, m.Index,
			r.RaftLog.LastTerm(), r.RaftLog.LastIndex()) {
		msg.Reject = true
	} else {
		// 投票
		r.Vote = m.From
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	r.votes[m.From] = !m.Reject
}

func isUpToDate(senderTerm, senderIndex, receiverTerm, receiverIndex uint64) bool {
	// 最后一条日志相比,任期高的新;任期相同,日志多(索引大)的新
	if senderTerm < receiverTerm {
		return false
	} else if senderTerm > receiverTerm {
		return true
	} else {
		return senderIndex >= receiverIndex
	}
}

func (r *Raft) handlePropose(m pb.Message) {
	r.RaftLog.Append(toEntrySlice(m.Entries))
	r.bcastAppend()
}

func toEntrySlice(ents []*pb.Entry) []pb.Entry {
	e := make([]pb.Entry, 0)
	for _, ent := range ents {
		e = append(e, *ent)
	}
	return e
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	// 如果拒绝的话,更新Next,重发
	if m.Reject {
		r.Prs[m.From].Next--
		r.sendAppend(m.From)
		return
	}
	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
