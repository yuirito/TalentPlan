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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
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
	// number of ticks since it reached last electionTimeout
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
	randomTimeout    int
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A)
	///x,_:=c.Storage.LastIndex()
	//y,_:=c.Storage.FirstIndex()
	//fmt.Printf("fi=%d,li=%d\n",y,x)
	raftlog := newLog(c.Storage)
	li := raftlog.LastIndex()
	prs := make(map[uint64]*Progress, len(c.peers))
	var i uint64
	for i = 1; i <= uint64(len(c.peers)); i++ {
		if i == c.ID {
			prs[i] = &Progress{Next: li + 1, Match: li}
		} else {
			prs[i] = &Progress{Next: li + 1, Match: 0}
		}
	}
	votes := make(map[uint64]bool, len(c.peers))
	for i = 1; i <= uint64(len(c.peers)); i++ {
		votes[i] = false
	}
	raft := &Raft{
		id:               c.ID,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		RaftLog:          raftlog,
		Prs:              prs,
		votes:            votes,
		randomTimeout:    0,
	}
	raft.becomeFollower(0, 0)
	state, _, _ := raft.RaftLog.storage.InitialState()
	raft.Term, raft.Vote, raft.RaftLog.committed = state.Term, state.Vote, state.Commit

	return raft

}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	//fmt.Printf("\nlen ent=%d\n",len(r.RaftLog.entries))

	li := r.RaftLog.LastIndex()
	if r.Prs[to].Next > li {
		return false
	}
	fi, _ := r.RaftLog.storage.FirstIndex()
	ents := make([]*pb.Entry, 0)
	if (li + 1) < r.Prs[to].Next {
		return false
	}
	if r.Prs[to].Next >= fi && r.Prs[to].Next <= li {
		ents = r.RaftLog.getEnts(r.Prs[to].Next)
	}

	idx := r.Prs[to].Next
	logTerm, err := r.RaftLog.Term(idx)
	if err != nil {
		panic(err)
	}

	m := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Entries: ents,
		Commit:  r.RaftLog.committed,
		LogTerm: logTerm,
		Index:   idx - 1,
	}
	r.msgs = append(r.msgs, m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	var m pb.Message
	m = pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, m)
}

// sendRequestVote sends a RequestVote RPC to the given peer.
func (r *Raft) sendRequestVote(to uint64) {
	m := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: r.RaftLog.LastTerm(),
		Index:   r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, m)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++

		//fmt.Printf("elapsed=%d,timeout=%d",r.heartbeatElapsed,r.heartbeatTimeout);
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat, From: r.id})
		}
	} else {
		r.electionElapsed++
		//fmt.Printf("elapsed=%d,timeout=%d",r.heartbeatElapsed,r.heartbeatTimeout);
		if r.electionElapsed >= r.electionTimeout+r.randomTimeout {
			r.electionElapsed = 0
			r.randomTimeout = rand.Intn(r.electionTimeout)
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup, From: r.id})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.electionElapsed = 0
	r.heartbeatElapsed = 0

}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.votes = make(map[uint64]bool, 0)
	r.Vote = r.id
	for i := 1; i < len(r.votes); i++ {
		r.votes[uint64(i)] = false
	}
	r.votes[r.id] = true
	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	for i, prs := range r.Prs {
		if i != r.id {
			prs.Next = r.RaftLog.LastIndex() + 1
			prs.Match = 0
		} else {
			prs.Match = r.RaftLog.LastIndex() + 1
			prs.Next = prs.Match + 1
		}
	}
	entry := pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
		Data:      nil,
	}
	r.RaftLog.entries = append(r.RaftLog.entries, entry)
	for i, _ := range r.Prs {
		if i != r.id {
			r.sendAppend(i)
		}
	}

}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	//fmt.Printf("id=%d,state=%d",r.id,r.State)
	switch r.State {
	case StateFollower:
		err := r.stepFollower(m)
		if err != nil {
			return err
		}

	case StateCandidate:
		err := r.stepCandidate(m)
		if err != nil {
			return err
		}
	case StateLeader:
		err := r.stepLeader(m)
		if err != nil {
			return err
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	switch r.State {
	case StateLeader:
		{
			li := r.RaftLog.LastIndex()
			//fmt.Printf("len m.entries=%d\n", len(m.Entries))
			ents := make([]pb.Entry, 0)
			for i := range m.Entries {
				m.Entries[i].Index = li + 1 + uint64(i)
				m.Entries[i].Term = r.Term
				ents = append(ents, *m.Entries[i])
				r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
				//fmt.Printf("len raftlog.entries=%d\n", len(r.RaftLog.entries))
			}
		}
	case StateFollower:
		{
			msg := pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				Term:    r.Term,
				Reject:  false,
				From:    r.id,
				To:      m.From,
			}
			logterm := m.LogTerm
			idx := m.Index
			li := r.RaftLog.LastIndex()
			if idx > r.RaftLog.LastIndex() {
				msg.Reject = true
			} else {
				rterm, _ := r.RaftLog.Term(idx)
				if rterm != logterm {
					msg.Reject = true
				}
			}

			r.RaftLog.stabled = li
			fi := r.RaftLog.FirstIndex()
			stable := true
			for i := range m.Entries {
				entidx := m.Entries[i].Index
				li = r.RaftLog.LastIndex()
				if entidx > (li + 1) {
					msg.Reject = true
					break
				}
				if entidx <= li {
					//if (entidx - fi) == uint64(len(r.RaftLog.entries)) {

					//	fmt.Printf("entidx=%d,fi=%d,li=%d,%d,len=%d\n", entidx, fi, li, entidx-fi, len(r.RaftLog.entries))
					//	fmt.Printf("%v\n", r.RaftLog.entries)
					//}
					enterm, _ := r.RaftLog.Term(entidx)
					if enterm != m.Entries[i].Term {

						r.RaftLog.entries[entidx-fi] = *m.Entries[i]
						if stable == true {
							r.RaftLog.stabled = entidx - 1
							stable = false
							r.RaftLog.entries = r.RaftLog.entries[0 : entidx-fi+1]
						}

					}
				} else {
					r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
				}

				//fmt.Printf("len raftlog.entries=%d\n", len(r.RaftLog.entries))
			}
			if msg.Reject == false {
				if r.RaftLog.committed < m.Commit {
					r.RaftLog.committed = m.Commit
				}

			}
			r.msgs = append(r.msgs, msg)

		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) handleRequestVote(m pb.Message) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    m.Term,
		Reject:  false,
	}
	if r.Term > m.Term {
		msg.Reject = true
	} else if r.Term < m.Term {
		r.becomeFollower(m.Term, m.From)
	} else {
		if m.LogTerm < r.RaftLog.LastTerm() || (m.LogTerm == r.RaftLog.LastTerm() && m.Index < r.RaftLog.LastIndex()) {
			msg.Reject = true
		}
		if r.Vote != m.From {
			msg.Reject = true
		}

	}
	if !msg.Reject {
		r.Vote = m.From
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) updateCommited() bool {
	matches := make([]uint64, len(r.RaftLog.entries))
	fi := r.RaftLog.FirstIndex()
	li := r.RaftLog.LastIndex()
	for i, prs := range r.Prs {
		//fmt.Printf("id=%d,match=%d,li=%d\n",i,prs.Match,li)
		if i == r.id {
			matches[li-fi]++
		} else {
			if prs.Match <= li && prs.Match >= fi {
				matches[prs.Match-fi]++
			}
		}
	}
	var sum uint64
	sum = 0
	for i := li; i >= fi; i-- {

		sum += matches[i-fi]
		if sum > uint64((len(r.Prs) / 2)) {
			term, _ := r.RaftLog.Term(i)
			if term == r.Term && i > r.RaftLog.committed {
				r.RaftLog.committed = i
				return true
			}
			break
		}
	}
	return false
}

func (r *Raft) stepLeader(m pb.Message) error {
	// Your Code Here (2C).
	//fmt.Printf("\n%d,msg.type=%d",r.id,m.MsgType)

	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		{
			if m.Term > r.Term {
				r.becomeFollower(m.Term, m.From)
			}
		}
	case pb.MessageType_MsgBeat:
		{
			//fmt.Printf("\n lenprs=%d\n",len(r.Prs))
			for i, _ := range r.Prs {
				if i != r.id {
					r.sendHeartbeat(i)
				}
			}
		}
	case pb.MessageType_MsgRequestVote:
		{
			r.handleRequestVote(m)
		}

	case pb.MessageType_MsgPropose:
		{

			r.handleAppendEntries(m)
			r.Prs[r.id].Match = r.RaftLog.LastIndex()
			r.Prs[r.id].Next = r.Prs[r.id].Match + 1

			if len(r.Prs) <= 1 {
				r.RaftLog.committed = r.RaftLog.LastIndex()
			}
			for i, _ := range r.Prs {
				if i != r.id {
					r.sendAppend(i)
				}

			}
		}
	case pb.MessageType_MsgAppendResponse:
		{
			if m.Reject {
				r.Prs[m.From].Next--
				if r.Prs[m.From].Next < 0 {
					r.Prs[m.From].Next = 0
				}
				r.sendAppend(m.From)

			} else {
				if m.Index > r.Prs[m.From].Match {
					r.Prs[m.From].Match = m.Index
					r.Prs[m.From].Next = m.Index + 1
					r.updateCommited()
				}

			}
		}

	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {

	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		{
			if m.Term >= r.Term {
				r.becomeFollower(m.Term, m.From)
			}
		}
	case pb.MessageType_MsgHeartbeat:
		{
			if m.Term >= r.Term {
				r.becomeFollower(m.Term, m.From)
			}
		}
	case pb.MessageType_MsgHup:
		{
			r.becomeCandidate()
			for i, _ := range r.Prs {
				//fmt.Printf("i=%d,r.id=%d",i,r.id)
				if i != r.id {
					r.sendRequestVote(i)
				}
			}
		}
	case pb.MessageType_MsgRequestVote:
		{
			r.handleRequestVote(m)
		}
	case pb.MessageType_MsgRequestVoteResponse:
		{
			if m.Term > r.Term {
				r.becomeFollower(m.Term, None)
				return nil
			}
			r.votes[m.From] = !m.Reject
			if len(r.votes) < (len(r.Prs) / 2) {
				return nil
			}
			granted := 0
			rejected := 0
			var i uint64
			for i = 1; i <= uint64(len(r.Prs)); i++ {
				v, voted := r.votes[i]
				if !voted {
					continue
				}
				if v {
					granted++
				} else {
					//rejected++
				}
			}
			if granted > (len(r.Prs) / 2) {
				r.becomeLeader()
				for i, _ := range r.Prs {
					if i != r.id {
						r.sendHeartbeat(i)
					}
				}
			} else if rejected > (len(r.Prs) / 2) {
				r.becomeFollower(m.Term, m.From)
			}

		}

	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	// Your Code Here (2C).

	//fmt.Printf("\n%d,msg.type=%d",r.id,m.MsgType)

	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		{
			if m.MsgType == pb.MessageType_MsgAppend {
				if r.Term > m.Term {
					err := errors.New("r.term>msg.term")
					return err
				}
				r.Term = m.Term
			}
			r.handleAppendEntries(m)
		}
	case pb.MessageType_MsgHeartbeat:
		{
			r.electionElapsed = 0
			r.Vote = None
		}
	case pb.MessageType_MsgHup:
		{
			r.becomeCandidate()
			for i, _ := range r.Prs {
				//fmt.Printf("i=%d,r.id=%d",i,r.id)
				if i != r.id {
					r.sendRequestVote(i)
				}
			}
		}
	case pb.MessageType_MsgRequestVote:
		{

			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				From:    r.id,
				To:      m.From,
				Reject:  false,
			}
			if m.LogTerm < r.RaftLog.LastTerm() {
				msg.Reject = true
			} else if m.LogTerm == r.RaftLog.LastTerm() && m.Index < r.RaftLog.LastIndex() {
				msg.Reject = true
			}
			if r.Vote == None {
				r.Vote = m.From
			} else if r.Vote != m.From {
				msg.Reject = true
			}
			if m.Term > r.Term {
				r.Term = m.Term
			}
			msg.Term = r.Term
			r.msgs = append(r.msgs, msg)
		}

	}
	return nil
}
