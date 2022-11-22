package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (Index, Term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
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
	cmpClient    = "CLIENT"
	cmpLogger    = "LOGGER"
	cmpCommit    = "COMMIT"
)

type ServerId int

type Term int

type LogIndex int

type ServerStateMachine interface {
	isLeader() bool
	processTick()
	processElectionTimeout() ServerStateMachine

	processIncomingRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) ServerStateMachine
	processRequestVoteResponse(serverId ServerId, args *RequestVoteArgs, reply *RequestVoteReply) ServerStateMachine

	processIncomingAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) ServerStateMachine
	processAppendEntriesResponse(serverId ServerId, args *AppendEntriesArgs, reply *AppendEntriesReply) ServerStateMachine

	processCommand(command interface{}) (index LogIndex, term Term)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	// R/O Thread-Safe data
	peers             []*Peer  // Peer nodes that encapsulates RPC endpoints
	me                ServerId // this peer's Index into peers[]
	electionTimeout   time.Duration
	heartBeatInterval time.Duration

	// Accessed atomically
	dead int32 // set by Kill()

	// Shared data
	mu                  sync.Mutex         // Lock to protect shared access to this peer's state
	nextElectionTimeout time.Time          // Next election timeout
	serverStateMachine  ServerStateMachine // Follower, Candidate, Leader State Logic
	persister           *Persister         // Object to hold this peer's persisted state
	currentTerm         Term               // Latest Term server has seen
	votedFor            ServerId           // Candidate that received vote in current Term, -1 is null

	log         Log
	commitIndex LogIndex      // Index of high-est log entry known
	lastApplied LogIndex      // Index of high-est log entry applied to state machine
	applyChn    chan ApplyMsg // Channel to apply log entries
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

func (rf *Raft) checkTerm(serverId ServerId, term Term) ServerStateMachine {
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

func (rf *Raft) dispatchAppendEntriesResponse(peer *Peer, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(rf.me, cmpRPC, "<=~= S%d Response to AppendEntries(%v) -> (%v)", peer.serverId, args, reply)
	rf.serverStateMachine = rf.
		checkTerm(peer.serverId, reply.Term).
		processAppendEntriesResponse(peer.serverId, args, reply)
}

func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex <= rf.lastApplied {
		DPrintf(rf.me, cmpCommit, "applyLog should only be called when commitIndex has advanced")
		panic("applyLog should only be called when commitIndex has advanced")
	}

	for index := rf.lastApplied + 1; index <= rf.commitIndex; index++ {
		DPrintf(rf.me, cmpCommit, "applying log index %d", index)

		entry := rf.log.getEntryAt(index)
		// TODO: get rid of this hack, refactor log to be 1-based index
		commandIndex := entry.Index + 1
		msg := ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: int(commandIndex)}

		rf.applyChn <- msg
		rf.lastApplied = index
	}
}

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

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.isLeader()
	if !isLeader {
		// DPrintf(rf.me, cmpClient, "Not leader - rejecting Command(%v)", Command)
		return 0, 0, isLeader
	}

	index, term := rf.serverStateMachine.processCommand(command)

	// TODO: get rid of this hack, refactor log to be 1-based index
	index = index + 1
	return int(index), int(term), isLeader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return int(rf.currentTerm), rf.isLeader()
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
		rf.peers = append(rf.peers, &Peer{raft: rf, endPoint: peer, serverId: ServerId(id)})
	}
	rf.persister = persister
	rf.me = ServerId(me)
	rf.votedFor = -1

	// Initialize timeouts
	rf.electionTimeout = time.Duration(rand.Intn(300)+500) * time.Millisecond
	rf.heartBeatInterval = time.Duration(110) * time.Millisecond
	rf.nextElectionTimeout = time.Now().Add(rf.electionTimeout)

	// initialize server state handler
	rf.serverStateMachine = &Follower{rf: rf}

	// initialize log
	rf.log = MakeLog(rf)
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.applyChn = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
