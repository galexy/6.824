package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
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
	mu                  sync.Mutex // Lock to protect shared access to this peer's state
	nextElectionTimeout time.Time  // Next election timeout
	serverState         ServerState
	persister           *Persister // Object to hold this peer's persisted state
	currentTerm         int        // Latest term server has seen
	votedFor            int        // Candidate that received vote in current term, -1 is null
	commitIndex         int        // Index of highest log entry known
	lastApplied         int        // Index of highest log entry applied to state machine

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
// have more recent info since it communicate the snapshot on applyCh.
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

//
// example RequestVote RPC handler.
//
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		time.Sleep(3 * time.Millisecond) // sleep for short period
		rf.checkElectionTimeout(time.Now())
	}
}

func (rf *Raft) checkElectionTimeout(now time.Time) {
	rf.mu.Lock()
	if now.After(rf.nextElectionTimeout) {
		DPrintf(rf.me, cmpTicker, "Election Timeout (%v) elapsed", rf.electionTimeout)

		rf.serverState = rf.serverState.electionTimeoutElapsed()
	}
	rf.mu.Unlock()
}

func (rf *Raft) resetTimer() {
	// Update the next timeout
	rf.nextElectionTimeout = time.Now().Add(rf.electionTimeout)
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

type Follower struct {
	rf *Raft
}

func (f *Follower) isLeader() bool {
	return false
}

func (f *Follower) electionTimeoutElapsed() ServerState {
	DPrintf(f.rf.me, cmpFollower, "Converting to Candidate")

	candidate := &Candidate{rf: f.rf}
	candidate.startElection()

	return candidate
}

func (f *Follower) requestVoteReceived(args *RequestVoteArgs, reply *RequestVoteReply) ServerState {
	DPrintf(f.rf.me, cmpFollower, "Vote Requested from C%d@T%d", args.CandidateId, args.Term)

	if f.rf.currentTerm > args.Term {
		DPrintf(f.rf.me, cmpFollower, "@T%d > C%d@ T%d, Rejecting", f.rf.currentTerm, args.CandidateId, args.Term)
		reply.Term = f.rf.currentTerm
		reply.VoteGranted = false
		return f
	}

	if f.rf.votedFor != -1 {
		DPrintf(f.rf.me, cmpFollower, "Already voted for S%d @ T%d", f.rf.votedFor, f.rf.currentTerm)
		reply.Term = f.rf.currentTerm
		reply.VoteGranted = false
		return f
	}

	// TODO: Add check for candidate log is at least up to date

	f.rf.votedFor = args.CandidateId
	DPrintf(f.rf.me, cmpFollower, "Granting vote to C%d @ T%d, Resetting Timer", args.CandidateId, args.Term)
	reply.Term = f.rf.currentTerm
	reply.VoteGranted = true
	f.rf.resetTimer()

	return f
}

func (f *Follower) voteResponseReceived(serverId int, args *RequestVoteArgs, reply *RequestVoteReply) ServerState {
	DPrintf(f.rf.me, cmpFollower, "<~~~ S%d Stale Response to RequestVote(%v). Ignoring", serverId, args)
	return f
}

func (f *Follower) appendEntriesReceived(args *AppendEntriesArgs, reply *AppendEntriesReply) ServerState {
	if f.rf.currentTerm > args.Term {
		DPrintf(f.rf.me, cmpFollower, "AppendEntries from S%d@T%d < S%d@T%d. Replying false.",
			args.LeaderId, args.Term, f.rf.me, f.rf.currentTerm)
		reply.Term = f.rf.currentTerm
		reply.Success = false
	} else {
		DPrintf(f.rf.me, cmpFollower, "AppendEntries from S%d@T%d. Replying true.", args.LeaderId, args.Term)
		reply.Term = f.rf.currentTerm
		reply.Success = true
		f.rf.resetTimer()
	}
	return f
}

func (f *Follower) shouldRetryRequestVote(args *RequestVoteArgs) bool {
	return false
}

type Candidate struct {
	rf    *Raft
	votes []bool
}

func (c *Candidate) isLeader() bool {
	return false
}

func (c *Candidate) startElection() {
	c.rf.currentTerm += 1
	c.rf.resetTimer()
	c.votes = make([]bool, len(c.rf.peers))
	c.votes[c.rf.me] = true
	c.rf.votedFor = c.rf.me

	DPrintf(c.rf.me, cmpCandidate, "Calling for election @T%d", c.rf.currentTerm)
	for i, _ := range c.rf.peers {
		if i == c.rf.me {
			continue
		}

		go c.rf.requestVote(i, c.rf.currentTerm, c.rf.me)
	}
}

func (c *Candidate) electionTimeoutElapsed() ServerState {
	DPrintf(c.rf.me, cmpCandidate, "Election Timeout @ T%d. Starting new election.", c.rf.currentTerm)
	c.startElection()
	return c
}

func (c *Candidate) requestVoteReceived(args *RequestVoteArgs, reply *RequestVoteReply) ServerState {
	DPrintf(c.rf.me, cmpCandidate, "Denying RequestVote from S%d@T%d. Trying to win election.",
		args.CandidateId, args.Term)

	reply.Term = c.rf.currentTerm
	reply.VoteGranted = false

	return c
}

func (c *Candidate) voteResponseReceived(serverId int, args *RequestVoteArgs, reply *RequestVoteReply) ServerState {
	if args.Term < c.rf.currentTerm {
		DPrintf(c.rf.me, cmpCandidate, "Received stale vote S%d @T%d < S%d@T%d. Ignoring.",
			args.CandidateId, args.Term, c.rf.me, c.rf.currentTerm)
		return c
	}

	if args.Term != c.rf.currentTerm || c.rf.currentTerm != reply.Term {
		panic(fmt.Sprintf("Candidate voteResponseReceived, expected terms to all match, currentTerm: %d, argsTerm: %d, reply: %d",
			c.rf.currentTerm, args.Term, reply.Term))
	}

	if !reply.VoteGranted {
		DPrintf(c.rf.me, cmpCandidate, "Denied vote from S%d@T%d", serverId, args.Term)
		return c
	}

	DPrintf(c.rf.me, cmpCandidate, "Received vote from S%d@T%d", serverId, args.Term)

	c.votes[serverId] = true
	var totalVotes = 0
	for _, vote := range c.votes {
		if vote {
			totalVotes += 1
		}
	}

	if totalVotes > len(c.votes)/2 {
		DPrintf(c.rf.me, cmpCandidate, "Received majority of votes. Promoting to Leader")
		leader := &Leader{rf: c.rf}
		go leader.heartbeat()

		return leader
	}

	return c
}

func (c *Candidate) appendEntriesReceived(args *AppendEntriesArgs, reply *AppendEntriesReply) ServerState {
	DPrintf(c.rf.me, cmpCandidate, "AppendEntries received from new Leader L%d@T%d. Converting to follower.",
		args.LeaderId, args.Term)

	f := &Follower{rf: c.rf}
	f.appendEntriesReceived(args, reply)
	return f
}

func (c *Candidate) shouldRetryRequestVote(args *RequestVoteArgs) bool {
	return c.rf.currentTerm == args.Term
}

type Leader struct {
	rf *Raft
}

func (l *Leader) isLeader() bool {
	return true
}

func (l *Leader) electionTimeoutElapsed() ServerState {
	l.rf.resetTimer()
	return l
}

func (l *Leader) requestVoteReceived(args *RequestVoteArgs, reply *RequestVoteReply) ServerState {
	DPrintf(l.rf.me, cmpLeader, "Denying RequestVote from C%d@T%d. Already Leader",
		args.CandidateId, args.Term)

	reply.Term = l.rf.currentTerm
	reply.VoteGranted = false

	return l
}

func (l *Leader) voteResponseReceived(serverId int, args *RequestVoteArgs, reply *RequestVoteReply) ServerState {
	DPrintf(l.rf.me, cmpLeader, "Ignoring RequestVote Response from S%d@T%d. Already Leader",
		serverId, args.Term)

	return l
}

func (l *Leader) appendEntriesReceived(args *AppendEntriesArgs, reply *AppendEntriesReply) ServerState {
	if args.Term >= l.rf.currentTerm {
		// 1) Shouldn't have a leader with the same term
		// 2) Newer term should have made current server a follower
		panic("Leader unexpectedly received AppendEntries for current or newer term.")
	}

	DPrintf(l.rf.me, cmpLeader, "S%d ~~> S%d AppendEntries Resp (T:%d, Success:false)",
		l.rf.me, args.LeaderId, l.rf.currentTerm)
	reply.Term = l.rf.currentTerm
	reply.Success = false

	return l
}

func (l *Leader) shouldRetryRequestVote(args *RequestVoteArgs) bool {
	return false
}

func (l *Leader) heartbeat() {
	// TODO: create a heartbeat id
	for l.rf.killed() == false {
		l.rf.mu.Lock()
		if l.rf.serverState != l {
			DPrintf(l.rf.me, cmpLeader, "No longer leader. Killing heartbeat.")
			l.rf.mu.Unlock()
			break
		}

		DPrintf(l.rf.me, cmpLeader, "Sending Heartbeat")
		for serverId, _ := range l.rf.peers {
			if serverId == l.rf.me {
				continue
			}

			go func(rf *Raft, serverId, term int) {
				args := &AppendEntriesArgs{Term: term, LeaderId: rf.me}
				reply := &AppendEntriesReply{}

				ok := l.rf.sendAppendEntries(serverId, args, reply)
				if !ok {
					// TODO: Handle error
				}

				rf.mu.Lock()
				rf.serverState = rf.checkTerm(serverId, reply.Term)
				rf.mu.Unlock()
			}(l.rf, serverId, l.rf.currentTerm)
		}

		l.rf.mu.Unlock()
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}
