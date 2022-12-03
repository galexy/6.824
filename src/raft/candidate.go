package raft

import "fmt"
import "6.824/logger"

type RequestVoteArgs struct {
	Term         Term     // Candidate's Term
	CandidateId  ServerId // ID of candidate requesting vote
	LastLogIndex LogIndex // index of Candidate's last log entry
	LastLogTerm  Term     // term of Candidate's last log entry
}

func (args *RequestVoteArgs) String() string {
	return fmt.Sprintf("C=%d, T=%d, LI=%d, LT=%d", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
}

type RequestVoteReply struct {
	Term        Term // Current Term of server - for candidate to update itself
	VoteGranted bool // Reply on whether candidate was granted vote
}

func (r *RequestVoteReply) String() string {
	return fmt.Sprintf("T%d, %v", r.Term, r.VoteGranted)
}

type Candidate struct {
	rf        *Raft
	votes     []bool
	campaigns int
}

func (c *Candidate) Debug(format string, a ...interface{}) {
	prefix := fmt.Sprintf("[S%d][%v] S%d ", c.rf.me, cmpCandidate, c.rf.me)
	logger.DPrintf(prefix+format, a...)
}

func (c *Candidate) isLeader() bool {
	return false
}

func (c *Candidate) startElection() {
	c.rf.currentTerm += 1
	c.votes = make([]bool, len(c.rf.peers))
	c.votes[c.rf.me] = true
	c.rf.votedFor = c.rf.me
	c.campaigns++

	if c.campaigns > 3 {
		// we are possibly in a deadlock for election due to timeouts
		// being in lock step
		c.rf.jitter()
	}

	c.rf.resetElectionTimeout()

	c.rf.persist()

	c.Debug("running a campaign(T=%d)", c.rf.currentTerm)
	for peerId, peer := range c.rf.peers {
		peerId := ServerId(peerId)
		if peerId == c.rf.me {
			continue
		}

		index, term := c.rf.log.lastLogEntry()
		go peer.callRequestVote(c.rf.currentTerm, c.rf.me, index, term)
	}
}

func (c *Candidate) processTick() {
	panic("Candidates only process election timeout")
}

func (c *Candidate) processElectionTimeout() ServerStateMachine {
	c.Debug("Election Timeout @ T%d. Starting new election.", c.rf.currentTerm)
	c.startElection()
	return c
}

func (c *Candidate) processIncomingRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) ServerStateMachine {
	c.Debug("Denying RequestVote(S%d, T=%d). Trying to win election.",
		args.CandidateId, args.Term)

	reply.Term = c.rf.currentTerm
	reply.VoteGranted = false

	return c
}

func (c *Candidate) processRequestVoteResponse(serverId ServerId, args *RequestVoteArgs, reply *RequestVoteReply) ServerStateMachine {
	if args.Term < c.rf.currentTerm {
		c.Debug("Received stale vote S%d @T%d < S%d@T%d. Ignoring.",
			args.CandidateId, args.Term, c.rf.me, c.rf.currentTerm)
		return c
	}

	if args.Term != c.rf.currentTerm || c.rf.currentTerm != reply.Term {
		panic(fmt.Sprintf("Candidate processRequestVoteResponse, expected terms to all match, currentTerm: %d, argsTerm: %d, reply: %d",
			c.rf.currentTerm, args.Term, reply.Term))
	}

	if !reply.VoteGranted {
		c.Debug("Denied vote from S%d@T%d", serverId, args.Term)
		return c
	}

	c.Debug("Received vote from S%d@T%d", serverId, args.Term)

	c.votes[serverId] = true
	var totalVotes = 0
	for _, vote := range c.votes {
		if vote {
			totalVotes += 1
		}
	}

	if totalVotes > len(c.votes)/2 {
		c.Debug("Received majority of votes. Promoting to Leader")
		leader := MakeLeader(c.rf)

		return leader
	}

	return c
}

func (c *Candidate) processIncomingAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) ServerStateMachine {
	c.Debug("AppendEntries received from new Leader L%d@T%d. Converting to follower.",
		args.LeaderId, args.Term)

	f := &Follower{rf: c.rf}
	f.processIncomingAppendEntries(args, reply)
	return f
}

func (c *Candidate) processAppendEntriesResponse(
	_ ServerId,
	_ *AppendEntriesArgs,
	_ *AppendEntriesReply) ServerStateMachine {

	c.Debug("Received Stale AppendEntries() Response. Ignoring.")
	return c
}

func (c *Candidate) processIncomingInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) ServerStateMachine {
	c.Debug("Received InstallSnapshot() Request. Not processing.")
	reply.Term = c.rf.currentTerm

	return c
}

func (c *Candidate) processInstallSnapshotResponse(serverId ServerId, args *InstallSnapshotArgs, reply *InstallSnapshotReply) ServerStateMachine {
	c.Debug("Received Stale InstallSnapshot() Response. Not processing.")

	return c
}

func (c *Candidate) processCommand(_ interface{}) (index LogIndex, term Term) {
	panic("Candidate should not be processing commands!")
}
