package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	cmpTicker    = "TICKER"
	cmpFollower  = "FOLLOW"
	cmpCandidate = "CANDID"
	cmpTerm      = "_TERM_"
	cmpRPC       = "SRVRPC"
	cmpLeader    = "LEADER"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type ServerStateMachine interface {
	isLeader() bool
	processTick()
	processElectionTimeout() ServerStateMachine

	processIncomingRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) ServerStateMachine
	shouldRetryFailedRequestVote(args *RequestVoteArgs) bool
	processRequestVoteResponse(serverId int, args *RequestVoteArgs, reply *RequestVoteReply) ServerStateMachine

	processIncomingAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) ServerStateMachine
	shouldRetryFailedAppendEntries(args *AppendEntriesArgs) bool
	processAppendEntriesResponse(serverId int, args *AppendEntriesArgs, reply *AppendEntriesReply) ServerStateMachine

	processCommand(command interface{}) (index int, term int)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	// R/O Thread-Safe data
	peers             []*Peer // Peer nodes that encapsulates RPC endpoints
	me                int     // this peer's index into peers[]
	electionTimeout   time.Duration
	heartBeatInterval time.Duration

	// Accessed atomically
	dead int32 // set by Kill()

	// Shared data
	mu                  sync.Mutex         // Lock to protect shared access to this peer's state
	nextElectionTimeout time.Time          // Next election timeout
	serverStateMachine  ServerStateMachine // Follower, Candidate, Leader State Logic
	persister           *Persister         // Object to hold this peer's persisted state
	currentTerm         int                // Latest term server has seen
	votedFor            int                // Candidate that received vote in current term, -1 is null

	log         Log
	commitIndex int // Index of highest log entry known
	lastApplied int // Index of highest log entry applied to state machine
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(3 * time.Millisecond) // sleep for short period
		now := time.Now()

		rf.mu.Lock()
		if rf.isLeader() {
			rf.serverStateMachine.processTick()
		} else if now.After(rf.nextElectionTimeout) {
			DPrintf(rf.me, cmpTicker, "Election Timeout (%v) elapsed", rf.electionTimeout)

			rf.serverStateMachine = rf.serverStateMachine.processElectionTimeout()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) resetElectionTimeout() {
	// Update the next timeout
	rf.nextElectionTimeout = time.Now().Add(rf.electionTimeout)
}

func (rf *Raft) checkTerm(serverId, term int) ServerStateMachine {
	if rf.currentTerm >= term {
		return rf.serverStateMachine
	}

	DPrintf(rf.me, cmpTerm, "@T%d < S%d@T%d. Converting to follower.", rf.currentTerm, serverId, term)
	rf.currentTerm = term
	rf.votedFor = -1
	return &Follower{rf: rf}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

func (rf *Raft) isLeader() bool {
	return rf.serverStateMachine.isLeader()
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(rf.me, cmpRPC, "<=-= S%d Receive RequestVote(%v)", args.CandidateId, args)
	rf.serverStateMachine = rf.
		checkTerm(args.CandidateId, args.Term).
		processIncomingRequestVote(args, reply)
}

func (rf *Raft) shouldRetryRequestVote(args *RequestVoteArgs) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.serverStateMachine.shouldRetryFailedRequestVote(args)
}

func (rf *Raft) dispatchRequestVoteResponse(peer *Peer, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(rf.me, cmpRPC, "<=~= S%d Response to RequestVote(%v) -> (%v)", peer.serverId, args, reply)
	rf.serverStateMachine = rf.
		checkTerm(peer.serverId, reply.Term).
		processRequestVoteResponse(peer.serverId, args, reply)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(rf.me, cmpRPC, "<=-= S%d Receive AppendEntries(%v)", args.LeaderId, args)
	rf.serverStateMachine = rf.
		checkTerm(args.LeaderId, args.Term).
		processIncomingAppendEntries(args, reply)
}

func (rf *Raft) shouldRetryFailedAppendEntries(args *AppendEntriesArgs) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.serverStateMachine.shouldRetryFailedAppendEntries(args)
}

func (rf *Raft) dispatchAppendEntriesResponse(peer *Peer, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(rf.me, cmpRPC, "<=~= S%d Response to AppendEntries(%v) -> (%v)", peer.serverId, args, reply)
	rf.serverStateMachine = rf.
		checkTerm(peer.serverId, reply.Term).
		processAppendEntriesResponse(peer.serverId, args, reply)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = rf.isLeader()
	if !isLeader {
		return
	}

	index, term = rf.serverStateMachine.processCommand(command)
	return
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.isLeader()
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peerClients[]. this
// server's port is peerClients[me]. all the servers' peerClients[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	for id, peer := range peers {
		rf.peers = append(rf.peers, &Peer{raft: rf, endPoint: peer, serverId: id})
	}
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1

	// Initialize timeouts
	rf.electionTimeout = time.Duration(rand.Intn(300)+500) * time.Millisecond
	rf.heartBeatInterval = time.Duration(110) * time.Millisecond
	rf.nextElectionTimeout = time.Now().Add(rf.electionTimeout)

	// initialize server state handler
	rf.serverStateMachine = &Follower{rf: rf}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
