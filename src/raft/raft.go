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
	"6.824/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
	"6.824/logger"
)

const (
	cmpFollower  = "FOLLOW"
	cmpCandidate = "CANDID"
	cmpRaft      = "_RAFT_"
	cmpLeader    = "LEADER"
	cmpLogger    = "LOGGER"
)

type ServerId int

type Term int

type LogIndex int

// ServerStateMachine defines an interface for /State Pattern/ objects
// that handle events in the distinct server states Follower, Candidate, and Leader.
type ServerStateMachine interface {
	isLeader() bool
	processTick()
	processElectionTimeout() ServerStateMachine

	processIncomingRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) ServerStateMachine
	processRequestVoteResponse(serverId ServerId, args *RequestVoteArgs, reply *RequestVoteReply) ServerStateMachine

	processIncomingAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) ServerStateMachine
	processAppendEntriesResponse(serverId ServerId, args *AppendEntriesArgs, reply *AppendEntriesReply) ServerStateMachine

	processIncomingInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) ServerStateMachine
	processInstallSnapshotResponse(serverId ServerId, args *InstallSnapshotArgs, reply *InstallSnapshotReply) ServerStateMachine

	processCommand(command interface{}) (index LogIndex, term Term)
}

//
// A Raft object represents a single peer in a cluster
//
type Raft struct {
	// R/O Thread-Safe data
	peers             []*Peer  // Peer nodes that encapsulates RPC endpoints
	me                ServerId // this peer's Index into peers[]
	electionTimeout   time.Duration
	heartBeatInterval time.Duration

	// Test specific
	dead      int32         // set by Kill()
	applyChn  chan ApplyMsg // Channel to apply log entries
	persister *Persister    // Object to hold this peer's persisted state

	// Shared data
	mu                  sync.Mutex         // Lock to protect shared access to this peer's state
	nextElectionTimeout time.Time          // Next election timeout
	serverStateMachine  ServerStateMachine // Follower, Candidate, Leader State Logic

	currentTerm Term     // Latest Term server has seen
	votedFor    ServerId // Candidate that received vote in current Term, -1 is null
	log         Log
	commitIndex LogIndex // Index of high-est log entry known
	lastApplied LogIndex // Index of high-est log entry applied to state machine
	committing  LogIndex // Index that is actively being commited
	snapshot    []byte   // latest snapshot if any
}

func (rf *Raft) Debug(format string, a ...interface{}) {
	prefix := fmt.Sprintf("[S%d][%v] S%d ", rf.me, cmpRaft, rf.me)
	logger.DPrintf(prefix+format, a...)
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
			rf.Debug("Election Timeout (%v) elapsed", rf.electionTimeout)

			rf.serverStateMachine = rf.serverStateMachine.processElectionTimeout()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) jitter() {
	timeout := time.Duration(rand.Intn(300)+500) * time.Millisecond
	rf.Debug("Randomly jitter(%v) election timeout", timeout)
	rf.electionTimeout = timeout
}

func (rf *Raft) resetElectionTimeout() {
	// Update the next timeout
	rf.nextElectionTimeout = time.Now().Add(rf.electionTimeout)
}

func (rf *Raft) checkTerm(serverId ServerId, term Term) ServerStateMachine {
	if rf.currentTerm >= term {
		return rf.serverStateMachine
	}

	rf.Debug("@T%d < S%d@T%d. Converting to follower.", rf.currentTerm, serverId, term)
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()

	return &Follower{rf: rf}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	rf.log.save(e)
	data := w.Bytes()

	rf.Debug("SaveStateAndSnapshot(CT=%d, VF=%d, D=len(%d), S=len(%d))",
		rf.currentTerm, rf.votedFor, len(data), len(rf.snapshot))
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var savedCurrentTerm Term

	if err := d.Decode(&savedCurrentTerm); err != nil {
		//panic(fmt.Sprintf("failed to read back server state, %v", err))
		panic(err)
	}

	var savedVotedFor ServerId
	if err := d.Decode(&savedVotedFor); err != nil {
		//panic(fmt.Sprintf("failed to read back server state, %v", err))
		panic(err)
	}

	rf.currentTerm = savedCurrentTerm
	rf.votedFor = savedVotedFor
	rf.Debug("loadingState(CT=%d, VF=%d)", rf.currentTerm, rf.votedFor)

	if err := rf.log.load(d); err != nil {
		panic(err)
	}
	rf.lastApplied = rf.log.getStartingIndex()
}

func (rf *Raft) isLeader() bool {
	return rf.serverStateMachine.isLeader()
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.Debug("<=-= S%d Receive RequestVote(%v)", args.CandidateId, args)
	rf.serverStateMachine = rf.
		checkTerm(args.CandidateId, args.Term).
		processIncomingRequestVote(args, reply)
}

func (rf *Raft) dispatchRequestVoteResponse(peer *Peer, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm {
		rf.Debug("<=~= S%d Old Response to RequestVote(%v) -> (%v). CT=%d. Dropping.",
			peer.serverId, args, reply, rf.currentTerm)
		return
	}

	rf.Debug("<=~= S%d Response to RequestVote(%v) -> (%v)", peer.serverId, args, reply)
	rf.serverStateMachine = rf.
		checkTerm(peer.serverId, reply.Term).
		processRequestVoteResponse(peer.serverId, args, reply)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.Debug("<=-= S%d Receive AppendEntries(%v)", args.LeaderId, args)
	rf.serverStateMachine = rf.
		checkTerm(args.LeaderId, args.Term).
		processIncomingAppendEntries(args, reply)
}

func (rf *Raft) dispatchAppendEntriesResponse(peer *Peer, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm {
		rf.Debug("<=~= S%d Old Response to AppendEntries(%v) -> (%v). CT=%d. Dropping.",
			peer.serverId, args, reply, rf.currentTerm)
		return
	}

	rf.Debug("<=~= S%d Response to AppendEntries(%v) -> (%v)", peer.serverId, args, reply)
	rf.serverStateMachine = rf.
		checkTerm(peer.serverId, reply.Term).
		processAppendEntriesResponse(peer.serverId, args, reply)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.Debug("<=-= S%d Receive InstallSnapshot(%v)", args.LeaderId, args)
	rf.serverStateMachine = rf.
		checkTerm(args.LeaderId, args.Term).
		processIncomingInstallSnapshot(args, reply)
}

func (rf *Raft) dispatchInstallSnapshotResponse(peer *Peer, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm {
		rf.Debug("<=~= S%d Old Response to InstallSnapshot(%v) -> (%v). CT=%d. Dropping.",
			peer.serverId, args, reply, rf.currentTerm)
		return
	}

	rf.Debug("<=~= S%d Response to InstallSnapshot(%v) -> (%v)", peer.serverId, args, reply)
	rf.serverStateMachine = rf.
		checkTerm(peer.serverId, reply.Term).
		processInstallSnapshotResponse(peer.serverId, args, reply)
}

func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex <= rf.lastApplied {
		rf.Debug("applyLog appears to be called after logs have been applied")
		return
	}

	if rf.committing > 0 {
		rf.Debug("Already actively commit %d")
		return
	}

	index := rf.lastApplied + 1
	rf.Debug("applying log index %d", index)
	rf.committing = index
	entry := rf.log.getEntryAt(index)
	msg := ApplyMsg{
		CommandValid: true,
		Command:      entry.Command,
		CommandIndex: int(entry.Index),
		CommandTerm:  int(entry.Term),
	}

	go func(rf *Raft, msg ApplyMsg) {
		// Don't lock here since applyChn can block. Lock after channel as received message
		rf.applyChn <- msg

		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.lastApplied = rf.committing
		rf.committing = 0
		if rf.lastApplied < rf.commitIndex {
			rf.Debug("More logs to apply - calling self")
			go rf.applyLog()
		}
	}(rf, msg)
}

//
// ApplyMsg is the payload that each Raft peer uses to single to the server
// as it becomes aware that successive log entries are committed via the applyCh
// passed to Make(). CommandValid is true to indicate that the ApplyMsg contains
//a newly committed log entry and false for other types of messages.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (a ApplyMsg) String() string {
	return fmt.Sprintf("command=%v, I=%d, C=%v", a.CommandValid, a.CommandIndex, a.Command)
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
		// logger.DPrintf(rf.me, cmpClient, "Not leader - rejecting Command(%v)", Command)
		return 0, 0, isLeader
	}

	index, term := rf.serverStateMachine.processCommand(command)
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
	rf.jitter()
	rf.heartBeatInterval = time.Duration(110) * time.Millisecond
	rf.nextElectionTimeout = time.Now().Add(rf.electionTimeout)

	// initialize server state handler
	rf.serverStateMachine = &Follower{rf: rf}

	// initialize log
	rf.log = MakeLog(rf)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyChn = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
