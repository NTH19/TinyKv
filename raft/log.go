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
	innerentries []pb.Entry
	entries []pb.Entry
	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	firstIndex,_:=storage.FirstIndex()
	lastIndex,_ :=storage.LastIndex()
	hs,_ ,_:=storage.InitialState()
	entry :=make([]pb.Entry,1)
	entries,_:=storage.Entries(firstIndex,lastIndex+1)
	if(entries != nil){
		for _,v:=range entries{
			entry=append(entry,v)
		}
	}
	outentries,_ :=storage.Entries(firstIndex,lastIndex+1)
	return &RaftLog{
		storage:storage,
		applied:0,
		innerentries:entry,
		entries:outentries,
		stabled:lastIndex,
		committed:hs.Commit,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if(len(l.innerentries)==1){
		return nil
	}
	return l.innerentries[l.stabled-l.FirstIndex()+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if(len(l.innerentries)==1){
		return nil
	}
	return l.innerentries[l.applied-l.FirstIndex()+1 : l.committed-l.FirstIndex()+1]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	return l.innerentries[len(l.innerentries)-1].Index
}
func (l *RaftLog) FirstIndex() uint64 {
	return l.innerentries[0].Index
}
// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	return l.innerentries[i-l.FirstIndex()].Term,nil
}
func (l *RaftLog) AfterChange(){
	if(len(l.innerentries)!=1){
		l.entries=l.innerentries[1:]
	}
}