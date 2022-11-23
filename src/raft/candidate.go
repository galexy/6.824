package raft

import "fmt"

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

	DPrintf(c.rf.me, cmpCandidate, "running a campaign(T=%d)", c.rf.currentTerm)
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
	DPrintf(c.rf.me, cmpCandidate, "Election Timeout @ T%d. Starting new election.", c.rf.currentTerm)
	c.startElection()
	return c
}

func (c *Candidate) processIncomingRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) ServerStateMachine {
	DPrintf(c.rf.me, cmpCandidate, "Denying RequestVote(S%d, T=%d). Trying to win election.",
		args.CandidateId, args.Term)

	reply.Term = c.rf.currentTerm
	reply.VoteGranted = false

	return c
}

func (c *Candidate) processRequestVoteResponse(serverId ServerId, args *RequestVoteArgs, reply *RequestVoteReply) ServerStateMachine {
	if args.Term < c.rf.currentTerm {
		DPrintf(c.rf.me, cmpCandidate, "Received stale vote S%d @T%d < S%d@T%d. Ignoring.",
			args.CandidateId, args.Term, c.rf.me, c.rf.currentTerm)
		return c
	}

	if args.Term != c.rf.currentTerm || c.rf.currentTerm != reply.Term {
		panic(fmt.Sprintf("Candidate processRequestVoteResponse, expected terms to all match, currentTerm: %d, argsTerm: %d, reply: %d",
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
		leader := MakeLeader(c.rf)

		return leader
	}

	return c
}

func (c *Candidate) processIncomingAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) ServerStateMachine {
	DPrintf(c.rf.me, cmpCandidate, "AppendEntries received from new Leader L%d@T%d. Converting to follower.",
		args.LeaderId, args.Term)

	f := &Follower{rf: c.rf}
	f.processIncomingAppendEntries(args, reply)
	return f
}

func (c *Candidate) processAppendEntriesResponse(
	_ ServerId,
	_ *AppendEntriesArgs,
	_ *AppendEntriesReply) ServerStateMachine {

	DPrintf(c.rf.me, cmpCandidate, "Received Stale AppendEntries Response. Ignoring.")
	return c
}

func (c *Candidate) processCommand(_ interface{}) (index LogIndex, term Term) {
	panic("Candidate should not be processing commands!")
}
