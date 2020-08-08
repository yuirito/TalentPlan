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
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
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
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	fi, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	li, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	entries, err := storage.Entries(fi, li+1)
	//fmt.Printf("fi=%d,li+1=%d,len=%d,%v\n",fi,li+1,len(entries),entries)
	raftlog := &RaftLog{
		storage: storage,
		stabled: li,
		entries: entries,
	}
	return raftlog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	ents := make([]pb.Entry, 0)
	li := l.LastIndex()
	for i := l.stabled; i < li; i++ {
		ents = append(ents, l.entries[i])
	}
	return ents
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		fi, err := l.storage.FirstIndex()
		if err != nil {
			panic(err)
		}
		return fi + uint64(len(l.entries)) - 1
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return i
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	fi, _ := l.storage.FirstIndex()
	li := l.LastIndex()
	if i > li || i < fi {
		return 0, nil
	}
	return l.entries[i-fi].Term, nil
}

func (l *RaftLog) LastTerm() uint64 {
	term, err := l.Term(l.LastIndex())
	if err != nil {
		return 0
	}
	return term
}

func (l *RaftLog) getEnts(i uint64) []*pb.Entry {
	// Your Code Here (2A).
	ents := make([]*pb.Entry, 0)
	fi, _ := l.storage.FirstIndex()
	li := l.LastIndex()
	fmt.Printf("start i=%d,len=%d", i, len(l.entries))
	for ; i <= li; i++ {
		ents = append(ents, &l.entries[i-fi])
	}

	return ents
}
