package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"

	"math/rand/v2"
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

type LogRecord struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                  sync.Mutex          // Lock to protect shared access to this peer's state
	peers               []*labrpc.ClientEnd // RPC end points of all peers
	persister           *tester.Persister   // Object to hold this peer's persisted state
	me                  int                 // this peer's index into peers[]
	electionTimout      time.Time
	peersType           PeerType
	voteCount           int
	isVoted             bool
	logReplicationCount int
	applyCh             chan raftapi.ApplyMsg

	CurrentTerm int
	Log         []LogRecord
	CommitIndex int
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.peersType == Leader
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
	Entries      []LogRecord
	LeaderCommit int
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
		rf.peersType = Follower
		timeout := getRandomElectionTimeout()
		rf.electionTimout = time.Now().Add(timeout)
		reply.Success = true
	} else {
		rf.isVoted = false
		rf.CurrentTerm = leaderArgs.Term
		rf.peersType = Follower
		timeout := getRandomElectionTimeout()
		rf.electionTimout = time.Now().Add(timeout)
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
	} else if rf.CurrentTerm < leaderArgs.Term {
		rf.peersType = Follower
		rf.CurrentTerm = leaderArgs.Term
		rf.isVoted = true
		rf.electionTimout = time.Now().Add(getRandomElectionTimeout())
		reply.VoteGranted = true
		reply.Term = rf.CurrentTerm
	} else if rf.CurrentTerm == leaderArgs.Term && !rf.isVoted {
		rf.isVoted = true
		rf.electionTimout = time.Now().Add(getRandomElectionTimeout())
		reply.VoteGranted = true
		reply.Term = rf.CurrentTerm
	} else {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	}
}

func (rf *Raft) AppendEntriesV2(leaderArgs *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.CurrentTerm > leaderArgs.Term {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	} else {
		prevLogIndex := leaderArgs.PrevLogIndex
		newIndex := prevLogIndex + 1
		if len(rf.Log) < prevLogIndex {
			timeout := getRandomElectionTimeout()
			rf.electionTimout = time.Now().Add(timeout)
			rf.CurrentTerm = leaderArgs.Term
			rf.peersType = Follower
			rf.CommitIndex = leaderArgs.LeaderCommit

			reply.Term = leaderArgs.Term
			reply.Success = false
			return
		}
		if len(rf.Log) == newIndex {
			if rf.Log[prevLogIndex].Term == leaderArgs.PrevLogTerm {
				timeout := getRandomElectionTimeout()
				rf.electionTimout = time.Now().Add(timeout)
				rf.CurrentTerm = leaderArgs.Term
				rf.peersType = Follower
				rf.CommitIndex = leaderArgs.LeaderCommit

				reply.Term = rf.CurrentTerm
				reply.Success = true

				if len(leaderArgs.Entries) != 0 {
					rf.Log = append(rf.Log, leaderArgs.Entries...)
				}
				return
			} else {
				timeout := getRandomElectionTimeout()
				rf.electionTimout = time.Now().Add(timeout)
				rf.CurrentTerm = leaderArgs.Term
				rf.peersType = Follower

				reply.Term = leaderArgs.Term
				reply.Success = false
				return
			}

		} else {
			if rf.Log[prevLogIndex].Term == leaderArgs.PrevLogTerm {
				timeout := getRandomElectionTimeout()
				rf.electionTimout = time.Now().Add(timeout)
				rf.CurrentTerm = leaderArgs.Term
				rf.peersType = Follower
				rf.CommitIndex = leaderArgs.LeaderCommit

				reply.Term = rf.CurrentTerm
				reply.Success = true

				if len(leaderArgs.Entries) != 0 {
					rf.Log = append(rf.Log, leaderArgs.Entries...)
				} else {
					rf.Log = rf.Log[0:newIndex]
				}
				return
			}

		}
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
	rf.mu.Lock()
	index := len(rf.Log)
	term := rf.CurrentTerm
	isLeader := rf.peersType == Leader
	leaderId := rf.me
	prevLogIndex := index - 1
	prevLogTerm := rf.Log[prevLogIndex].Term

	logRecord := LogRecord{Term: rf.CurrentTerm, Command: command}
	rf.Log = append(rf.Log, logRecord)
	rf.logReplicationCount = 1

	rf.mu.Unlock()

	if !isLeader {
		return index, term, isLeader
	}

	for _, peer := range rf.peers {
		if peer == rf.peers[rf.me] {
			continue
		}
		go func(peer *labrpc.ClientEnd) {
			appendEntriesReply := &AppendEntriesReply{}
			appendEntriesArgs := &AppendEntriesArgs{
				Term:         term,
				LeaderId:     leaderId,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      []LogRecord{logRecord},
			}

			ok := peer.Call("Raft.AppendEntriesV2", &appendEntriesArgs, &appendEntriesReply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if appendEntriesReply.Term > rf.CurrentTerm {
					rf.voteCount = 0
					rf.isVoted = false
					rf.CurrentTerm = appendEntriesReply.Term
					rf.peersType = Follower
					rf.electionTimout = time.Now().Add(getRandomElectionTimeout())
				}
				if appendEntriesReply.Success && appendEntriesReply.Term == rf.CurrentTerm {
					rf.logReplicationCount += 1

					if rf.logReplicationCount > len(rf.peers)/2 {
						rf.CommitIndex += 1
						rf.logReplicationCount = 0
						go func() {
							rf.applyCh <- raftapi.ApplyMsg{CommandValid: true, Command: logRecord.Command, CommandIndex: len(rf.Log) - 1}
						}()
					}
				}
			}

		}(peer)
	}

	return index, term, isLeader
}

func (rf *Raft) ticker() {
	for true {
		rf.mu.Lock()
		if time.Now().After(rf.electionTimout) && (rf.peersType == Candidate || rf.peersType == Follower) {
			rf.peersType = Candidate
			rf.CurrentTerm += 1
			rf.voteCount = 1
			rf.isVoted = true
			rf.electionTimout = time.Now().Add(getRandomElectionTimeout())

			go rf.requestsForVotes(rf.CurrentTerm, rf.voteCount)
		}
		rf.mu.Unlock()
		ms := 100 + rand.IntN(50)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) requestsForVotes(currentTerm, voteCount int) {
	for _, peer := range rf.peers {
		if peer == rf.peers[rf.me] {
			continue
		}
		go func(peer *labrpc.ClientEnd) {
			requestVoteReply := &RequestVoteReply{}
			ok := peer.Call("Raft.RequestVote", &RequestVoteArgs{Term: currentTerm, CandidateId: rf.me}, &requestVoteReply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if requestVoteReply.VoteGranted && requestVoteReply.Term == rf.CurrentTerm {
					voteCount += 1
					rf.voteCount = voteCount
					if rf.voteCount > len(rf.peers)/2 {
						rf.peersType = Leader
						go rf.sendHealthChecksToPeers(rf.CurrentTerm)
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) sendHealthChecksToPeers(currentTerm int) {
	for _, peer := range rf.peers {
		if peer == rf.peers[rf.me] {
			continue
		}
		go func(peer *labrpc.ClientEnd) {
			appendEntriesReply := &AppendEntriesReply{}
			ok := peer.Call("Raft.AppendEntries", &AppendEntriesArgs{Term: currentTerm, LeaderId: rf.me}, &appendEntriesReply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if appendEntriesReply.Term > rf.CurrentTerm {
					rf.isVoted = false
					rf.CurrentTerm = appendEntriesReply.Term
					rf.peersType = Follower
					rf.electionTimout = time.Now().Add(getRandomElectionTimeout())
				}
			}
		}(peer)
	}
}

func (rf *Raft) AmILeader(peers []*labrpc.ClientEnd) {
	for {
		rf.mu.Lock()
		if rf.peersType == Leader {
			go rf.sendHealthChecksToPeers(rf.CurrentTerm)
		}
		rf.mu.Unlock()
		ms := 100 + rand.IntN(50)
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

	initialLogRecord := LogRecord{Term: 0, Command: "fake command"}
	rf := &Raft{electionTimout: time.Now(), peersType: Follower, Log: []LogRecord{initialLogRecord}}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.AmILeader(peers)

	return rf
}
func getRandomElectionTimeout() time.Duration {
	return time.Duration(350+rand.IntN(151)) * time.Millisecond
}
