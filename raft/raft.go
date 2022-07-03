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
	"sort"
	"time"
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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	hs ,cs,_ :=c.Storage.InitialState()
	r:= &Raft{
		id:c.ID,
		Term:hs.Term,
		Vote:hs.Vote,
		State: StateFollower,
		votes:make(map[uint64]bool),
		Lead:None,
		Prs:make(map[uint64]*Progress),
		heartbeatElapsed:0,
		electionElapsed:0,
		electionTimeout:c.ElectionTick,
		heartbeatTimeout:c.HeartbeatTick,
		RaftLog:newLog(c.Storage),
	}
	if c.peers== nil{
		c.peers=cs.Nodes
	}
	for _,v := range c.peers{
		r.votes[v]=false
		r.Prs[v]=&Progress{}

	}
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	prevIndex := r.Prs[to].Next - 1
	prevLogTerm, _ := r.RaftLog.Term(prevIndex)
	var entries []*pb.Entry
	n := r.RaftLog.LastIndex() + 1
	firstIndex := r.RaftLog.FirstIndex()
	for i := r.Prs[to].Next; i < n; i++ {
		entries = append(entries, &r.RaftLog.innerentries[i-firstIndex])
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		LogTerm: prevLogTerm,
		Index:   prevIndex,
		Entries: entries,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVote(to uint64){
	lastIndex :=r.RaftLog.LastIndex()
	lastTerm,_:=r.RaftLog.Term(lastIndex)
	msg :=pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:		 to,
		Term:    r.Term,
		Index: lastIndex,
		LogTerm: lastTerm,
	}
	r.msgs= append(r.msgs,msg)
}
// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State{
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed==r.heartbeatTimeout{
			r.Step(pb.Message{MsgType:pb.MessageType_MsgBeat})
		}
	case StateFollower:
		fallthrough
	case StateCandidate:
		r.electionElapsed++
		if r.electionElapsed == r.electionTimeout{
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State=StateFollower
	r.Term=term
	r.Vote=None
	r.Lead=lead
	r.heartbeatElapsed=0
	r.electionElapsed=0
	rand.Seed(time.Now().UnixNano())
	r.electionElapsed-=rand.Intn(r.electionTimeout)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State=StateCandidate
	r.Vote=r.id
	r.Term++
	r.Lead = None
	r.heartbeatElapsed=0
	for i :=range r.votes{
		r.votes[i]=false
	}
	r.votes[r.id]=true
	r.electionElapsed=0
	rand.Seed(time.Now().UnixNano())
	r.electionElapsed-=rand.Intn(r.electionTimeout)
}
// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.State = StateLeader
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.Lead = r.id
	r.Vote=r.Lead

	lastIndex :=r.RaftLog.LastIndex()
	for id:= range r.Prs{
		r.Prs[id].Match=0
		r.Prs[id].Next=lastIndex+1
	}
	noopEntry :=pb.Entry{
		Term:r.Term,
		Index:r.RaftLog.LastIndex()+1,
		Data:nil,
	}
	r.RaftLog.innerentries=append(r.RaftLog.innerentries,noopEntry)
	r.RaftLog.AfterChange()
	r.Prs[r.id].Match=r.RaftLog.LastIndex()
	r.Prs[r.id].Next=r.RaftLog.LastIndex()+1
	for id:= range r.Prs{
		if(id!=r.id){
			r.sendAppend(id)
		}
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.FHandleMessage(m)
	case StateCandidate:
		r.CHandleMessage(m)
	case StateLeader:
		r.LHandleMessage(m)

	}
	return nil
}
func (r *Raft) FHandleMessage(m pb.Message){
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		if(len(r.votes)==1){
			r.becomeLeader()
			return
		}
		allnodes :=nodes(r)
		for _,v :=range allnodes{
			if v!= r.id{
				r.sendRequestVote(v)
			}
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		msg:=pb.Message{
			MsgType:pb.MessageType_MsgRequestVoteResponse,
			From:m.To,
			To:m.From,
		}
		if r.Term>m.Term{
			msg.Term=r.Term
			msg.Reject=true
			r.msgs=append(r.msgs,msg)
			return
		}
		if(r.Term<m.Term){
			r.Term=m.Term
			r.Vote=None
		}
		msg.Term=m.Term
		if (r.Vote==None || r.Vote==m.From){
			lastIndex := r.RaftLog.LastIndex()
			lastTerm ,_:=r.RaftLog.Term(lastIndex)
			if(lastTerm< m.LogTerm || (lastTerm==m.LogTerm && m.Index>=lastIndex)){
				msg.Reject=false
				r.Vote=m.From
				r.msgs=append(r.msgs,msg)
				return
			}
		}
		msg.Reject=true
		r.msgs=append(r.msgs,msg)
		return
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppendResponse: 
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeatResponse: 
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
}

func (r *Raft) CHandleMessage(m pb.Message){
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		if(len(r.votes)==1){
			r.becomeLeader()
			return
		}
		allnodes :=nodes(r)
		for _,v :=range allnodes{
			if v!= r.id{
				r.sendRequestVote(v)
			}
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		if m.Term>r.Term{
			r.becomeFollower(m.Term,None)
			r.Step(m)
			return
		}
	case pb.MessageType_MsgRequestVoteResponse:
		if m.Reject==true{
			r.votes[m.From]=false
			if m.Term>r.Term{
				r.becomeFollower(m.Term,None)
			}
		}else{
			r.votes[m.From]=true
			counts :=0
			for _,v :=range r.votes{
				if v==true{
					counts++
				}
			}
			if counts>len(r.votes)/2{
				r.becomeLeader()
			}
		}
	case pb.MessageType_MsgHeartbeat:
	case pb.MessageType_MsgBeat: 
	case pb.MessageType_MsgPropose:
	case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeatResponse:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
}

func (r *Raft) LHandleMessage(m pb.Message){
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		allnodes :=nodes(r)
		for _,v:=range allnodes{
			if v!= r.id{
				r.sendHeartbeat(v)
			}
		}
		r.heartbeatElapsed=0
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse: 
		if(m.Reject==true){
			if(m.Term>r.Term){
				r.becomeFollower(m.Term,None)
				return
			}
			r.Prs[m.From].Next=m.Index
			r.sendAppend(m.From)
		}
		r.Prs[m.From].Match = m.Index
		r.Prs[m.From].Next = m.Index+1
		match := make(uint64Slice, len(r.Prs))
		i :=0
		for _, node := range r.Prs {
			match[i] = node.Match
			i++
		}
		sort.Sort(match)
		counts :=uint64(0)
		if(len(match)%2==0){
			counts = uint64(len(match)/2-1)
		}else{
			counts = uint64((len(match)+1)/2-1)
		}
		if match[counts] > r.RaftLog.committed {
			logTerm, _ := r.RaftLog.Term(match[counts])
			if logTerm == r.Term {
				r.RaftLog.committed = match[counts]
			}
		}
	case pb.MessageType_MsgRequestVote:
		if m.Term>r.Term{
			r.becomeFollower(m.Term,None)
			r.Step(m)
			return
		}
	case pb.MessageType_MsgPropose:
		for i,v :=range m.Entries{ 
			v.Term=r.Term
			v.Index=r.RaftLog.LastIndex()+uint64(i)+1
			r.RaftLog.innerentries=append(r.RaftLog.innerentries,*v)
			r.RaftLog.AfterChange()
		}
		r.Prs[r.id].Next=r.RaftLog.LastIndex()+1
		r.Prs[r.id].Match=r.RaftLog.LastIndex()
		for node :=range r.Prs{
			if node != r.id{
				r.sendAppend(node)
			}
		}
		if len(r.Prs)==1{
			r.RaftLog.committed=r.RaftLog.LastIndex()
		}
	case pb.MessageType_MsgHeartbeatResponse: 
	case pb.MessageType_MsgHup:
	case pb.MessageType_MsgRequestVoteResponse: 
	case pb.MessageType_MsgSnapshot:
	case pb.MessageType_MsgHeartbeat:
	case pb.MessageType_MsgTransferLeader:
	case pb.MessageType_MsgTimeoutNow:
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	if(r.Term<=m.Term){
		r.becomeFollower(m.Term,m.From)
	}else{
		msg := pb.Message{
			MsgType: pb.MessageType_MsgAppendResponse,
			From:    m.To,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		}
		r.msgs = append(r.msgs, msg)
		return
	}
	lastIndex := r.RaftLog.LastIndex()
	if m.Index <= lastIndex {
		prevTerm ,_:= r.RaftLog.Term(m.Index)
		if m.LogTerm == prevTerm{
			base :=r.RaftLog.FirstIndex()
			r.RaftLog.innerentries=r.RaftLog.innerentries[0:m.Index-base+1]
			r.RaftLog.AfterChange()
			for _,v:=range m.Entries{
				r.RaftLog.innerentries=append(r.RaftLog.innerentries,*v)
				r.RaftLog.AfterChange()
			}
			msg := pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  false,
				Index:   r.RaftLog.LastIndex(),
			}
			if(r.RaftLog.stabled>=m.Index+1){
				r.RaftLog.stabled=m.Index
			}
			if m.Commit > r.RaftLog.committed {
				r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
			}
			r.msgs = append(r.msgs, msg)
			return
		}else{
			msg := pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				From:    m.To,
				To:      m.From,
				Term:    r.Term,
				Reject:  true,
				Index: m.Index-1,
			}
			r.msgs = append(r.msgs, msg)
			return
		}
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    m.To,
		To:      m.From,
		Term:    r.Term,
		Reject:  true,
		Index: lastIndex,
	}
	r.msgs = append(r.msgs, msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	msg:=pb.Message{
		MsgType:pb.MessageType_MsgHeartbeatResponse,
  		From:m.To,
		To:m.From,
	}
	if r.Term>m.Term {
		msg.Reject=true
	}else {
		msg.Reject=false
		r.electionElapsed=0
		if r.State != StateFollower{
			r.becomeFollower(m.Term,m.From)
		}
	}
	msg.Term=r.Term
	r.msgs=append(r.msgs,msg)
	return
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
