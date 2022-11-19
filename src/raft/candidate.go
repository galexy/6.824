package raft

import "fmt"

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
	for i := range c.rf.peers {
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
