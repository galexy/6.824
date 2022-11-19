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
	"fmt"
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

type ServerState interface {
	isLeader() bool
	electionTimeoutElapsed() ServerState
	requestVoteReceived(args *RequestVoteArgs, reply *RequestVoteReply) ServerState
	voteResponseReceived(serverId int, args *RequestVoteArgs, reply *RequestVoteReply) ServerState
	appendEntriesReceived(args *AppendEntriesArgs, reply *AppendEntriesReply) ServerState
	shouldRetryRequestVote(args *RequestVoteArgs) bool
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	// R/O Thread-Safe data
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	me                int                 // this peer's index into peers[]
	electionTimeout   time.Duration
	heartBeatInterval int

	// Accessed atomically
	dead int32 // set by Kill()

	// Shared data
	mu                  sync.Mutex  // Lock to protect shared access to this peer's state
	nextElectionTimeout time.Time   // Next election timeout
	serverState         ServerState // Follower, Candidate, Leader State Logic
	persister           *Persister  // Object to hold this peer's persisted state
	currentTerm         int         // Latest term server has seen
	votedFor            int         // Candidate that received vote in current term, -1 is null
	commitIndex         int         // Index of highest log entry known
	lastApplied         int         // Index of highest log entry applied to state machine

}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(3 * time.Millisecond) // sleep for short period
		now := time.Now()

		rf.mu.Lock()
		if now.After(rf.nextElectionTimeout) {
			DPrintf(rf.me, cmpTicker, "Election Timeout (%v) elapsed", rf.electionTimeout)

			rf.serverState = rf.serverState.electionTimeoutElapsed()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) resetTimer() {
	// Update the next timeout
	rf.nextElectionTimeout = time.Now().Add(rf.electionTimeout)
}

func (rf *Raft) checkTerm(serverId, term int) ServerState {
	if rf.currentTerm >= term {
		return rf.serverState
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// had more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term        int // Candidate's Term
	CandidateId int // ID of candidate requesting vote
}

func (args *RequestVoteArgs) String() string {
	return fmt.Sprintf("C%d, T%d", args.CandidateId, args.Term)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int  // Current Term of server - for candidate to update itself
	VoteGranted bool // Reply on whether candidate was granted vote
}

func (r *RequestVoteReply) String() string {
	return fmt.Sprintf("T%d, %v", r.Term, r.VoteGranted)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	DPrintf(rf.me, cmpRPC, "<=-= S%d Receive RequestVote(%v)", args.CandidateId, args)
	rf.serverState = rf.checkTerm(args.CandidateId, args.Term)
	rf.serverState = rf.serverState.requestVoteReceived(args, reply)
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVoteRPC(serverId int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf(rf.me, cmpRPC, "=-=> S%d Send RequestVote(%v)", serverId, args)
	ok := rf.peers[serverId].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) requestVote(serverId, term, candidateId int) {
	args := &RequestVoteArgs{Term: term, CandidateId: candidateId}
	reply := &RequestVoteReply{}

	for !rf.sendRequestVoteRPC(serverId, args, reply) {
		DPrintf(rf.me, cmpRPC, "=/=> S%d Failed RequestVote()", serverId)

		if rf.shouldRetryRequestVote(args) {
			DPrintf(rf.me, cmpRPC, "=~=> S%d Retrying RequestVote())", serverId)
		} else {
			DPrintf(rf.me, cmpRPC, "=/=> S%d Dropping RequestVote()", serverId)
			return
		}
	}

	DPrintf(rf.me, cmpRPC, "<=~= S%d Response to RequestVote(%v) -> (%v)", serverId, args, reply)

	rf.mu.Lock()
	rf.serverState = rf.checkTerm(serverId, reply.Term)
	rf.serverState = rf.serverState.voteResponseReceived(serverId, args, reply)
	rf.mu.Unlock()
}

func (rf *Raft) shouldRetryRequestVote(args *RequestVoteArgs) bool {
	var retry bool

	rf.mu.Lock()
	retry = rf.serverState.shouldRetryRequestVote(args)
	rf.mu.Unlock()

	return retry
}

type AppendEntriesArgs struct {
	Term     int // Leader's term
	LeaderId int
}

func (a *AppendEntriesArgs) String() string {
	return fmt.Sprintf("L%d, T%d", a.LeaderId, a.Term)
}

type AppendEntriesReply struct {
	Term    int // currentTerm, for leader to update itself
	Success bool
}

func (r *AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, %v", r.Term, r.Success)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf(rf.me, cmpRPC, "<=-= S%d Receive AppendEntries(%v)", args.LeaderId, args)
	rf.serverState = rf.checkTerm(args.LeaderId, args.Term)
	rf.serverState = rf.serverState.appendEntriesReceived(args, reply)
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool

	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.serverState.isLeader()
	rf.mu.Unlock()

	return term, isLeader
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
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
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
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1

	// Initialize timeouts
	rf.electionTimeout = time.Duration(rand.Intn(300)+500) * time.Millisecond
	rf.heartBeatInterval = 110
	rf.nextElectionTimeout = time.Now().Add(rf.electionTimeout)

	// initialize server state handler
	rf.serverState = &Follower{rf: rf}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
