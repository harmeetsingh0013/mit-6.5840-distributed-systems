package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type PeerType string

const (
	Leader    PeerType = "leader"
	Follower  PeerType = "follower"
	Candidate PeerType = "candidate"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu             sync.Mutex          // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // RPC end points of all peers
	persister      *tester.Persister   // Object to hold this peer's persisted state
	me             int                 // this peer's index into peers[]
	CurrentTerm    int
	Log            []string
	ElectionTimout time.Time
	PeersType      PeerType
	VoteCount      int
	isVoted        bool
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.PeersType == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(leaderArgs *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.CurrentTerm > leaderArgs.Term {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	} else if rf.CurrentTerm == leaderArgs.Term {
		reply.Term = rf.CurrentTerm
		rf.CurrentTerm = leaderArgs.Term
		rf.PeersType = Follower
		timeout := getRandomElectionTimeout()
		rf.ElectionTimout = time.Now().Add(timeout)
		reply.Success = true
	} else {
		rf.isVoted = false
		rf.CurrentTerm = leaderArgs.Term
		rf.PeersType = Follower
		timeout := getRandomElectionTimeout()
		rf.ElectionTimout = time.Now().Add(timeout)
		reply.Success = true
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(leaderArgs *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.CurrentTerm > leaderArgs.Term {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	} else if rf.CurrentTerm == leaderArgs.Term && !rf.isVoted {
		rf.isVoted = true
		reply.VoteGranted = true
		reply.Term = rf.CurrentTerm
	} else if rf.CurrentTerm < leaderArgs.Term {
		rf.CurrentTerm = leaderArgs.Term
		reply.VoteGranted = true
		rf.isVoted = true
		rf.PeersType = Follower
		reply.Term = rf.CurrentTerm
	} else {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

func (rf *Raft) ticker() {
	for true {
		rf.mu.Lock()
		if time.Now().After(rf.ElectionTimout) && rf.PeersType == Candidate {
			rf.CurrentTerm += 1
			rf.VoteCount += 1
			rf.isVoted = true
			rf.ElectionTimout = time.Now().Add(getRandomElectionTimeout())

			go requestsForVotes(rf.peers, rf)
		}
		rf.mu.Unlock()
		ms := 100 + rand.Intn(50)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func requestsForVotes(peers []*labrpc.ClientEnd, rf *Raft) {
	rf.mu.Lock()
	currentTerm := rf.CurrentTerm
	currentVoteCount := rf.VoteCount
	rf.mu.Unlock()
	for _, peer := range peers {
		if peer == peers[rf.me] {
			continue
		}
		requestVoteReply := &RequestVoteReply{}
		ok := peer.Call("Raft.RequestVote", &RequestVoteArgs{Term: currentTerm, CandidateId: rf.me}, &requestVoteReply)
		if ok {
			rf.mu.Lock()
			if requestVoteReply.VoteGranted && requestVoteReply.Term == currentTerm {
				currentVoteCount += 1
				rf.VoteCount = currentVoteCount

				if rf.VoteCount > len(peers)/2 {
					rf.PeersType = Leader
					go sendHealthChecksToPeers(peers, rf)
					rf.mu.Unlock()
					continue
				}
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
			}
		}
	}
}

func sendHealthChecksToPeers(peers []*labrpc.ClientEnd, rf *Raft) {
	rf.mu.Lock()
	currentTerm := rf.CurrentTerm
	rf.mu.Unlock()
	for _, peer := range peers {
		if peer == peers[rf.me] {
			continue
		}
		appendEntriesReply := &AppendEntriesReply{}
		peer.Call("Raft.AppendEntries", &AppendEntriesArgs{Term: currentTerm, LeaderId: rf.me}, &appendEntriesReply)
	}
}

func (rf *Raft) AmILeader(peers []*labrpc.ClientEnd) {
	for {
		rf.mu.Lock()
		if rf.PeersType == Leader {
			go sendHealthChecksToPeers(peers, rf)
		} else if rf.PeersType == Follower && time.Now().After(rf.ElectionTimout) {
			rf.PeersType = Candidate
		}
		rf.mu.Unlock()
		ms := 100 + rand.Intn(50)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	electionTimeout := time.Now()
	rf := &Raft{ElectionTimout: electionTimeout, PeersType: Follower}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.AmILeader(peers)

	return rf
}
func getRandomElectionTimeout() time.Duration {
	return time.Duration(350+rand.Intn(151)) * time.Millisecond
}
