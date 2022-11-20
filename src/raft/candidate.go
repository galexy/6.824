package raft

import "fmt"

type RequestVoteArgs struct {
	Term        int // Candidate's Term
	CandidateId int // ID of candidate requesting vote
}

func (args *RequestVoteArgs) String() string {
	return fmt.Sprintf("C%d, T%d", args.CandidateId, args.Term)
}

type RequestVoteReply struct {
	Term        int  // Current Term of server - for candidate to update itself
	VoteGranted bool // Reply on whether candidate was granted vote
}

func (r *RequestVoteReply) String() string {
	return fmt.Sprintf("T%d, %v", r.Term, r.VoteGranted)
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
	c.rf.resetElectionTimeout()
	c.votes = make([]bool, len(c.rf.peers))
	c.votes[c.rf.me] = true
	c.rf.votedFor = c.rf.me

	DPrintf(c.rf.me, cmpCandidate, "Calling for election @T%d", c.rf.currentTerm)
	for peerId, peer := range c.rf.peers {
		if peerId == c.rf.me {
			continue
		}

		go peer.callRequestVote(c.rf.currentTerm, c.rf.me)
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
	DPrintf(c.rf.me, cmpCandidate, "Denying RequestVote from S%d@T%d. Trying to win election.",
		args.CandidateId, args.Term)

	reply.Term = c.rf.currentTerm
	reply.VoteGranted = false

	return c
}

func (c *Candidate) shouldRetryFailedRequestVote(args *RequestVoteArgs) bool {
	return c.rf.currentTerm == args.Term
}

func (c *Candidate) processRequestVoteResponse(serverId int, args *RequestVoteArgs, reply *RequestVoteReply) ServerStateMachine {
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
		leader := &Leader{rf: c.rf}
		leader.initHeartbeats()

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

func (c *Candidate) shouldRetryFailedAppendEntries(_ *AppendEntriesArgs) bool {
	return false
}

func (c *Candidate) processAppendEntriesResponse(
	_ int,
	_ *AppendEntriesArgs,
	_ *AppendEntriesReply) ServerStateMachine {

	DPrintf(c.rf.me, cmpCandidate, "Received Stale AppendEntries Response. Ignoring.")
	return c
}

func (c *Candidate) processCommand(command interface{}) (index int, term int) {
	panic("Candidate should not be processing commands!")
}
