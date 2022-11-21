package raft

type Follower struct {
	rf *Raft
}

func (f *Follower) isLeader() bool {
	return false
}

func (f *Follower) processTick() {
	panic("Followers only process election timeout")
}

func (f *Follower) processElectionTimeout() ServerStateMachine {
	DPrintf(f.rf.me, cmpFollower, "Converting to Candidate")

	candidate := &Candidate{rf: f.rf}
	candidate.startElection()

	return candidate
}

func (f *Follower) processIncomingRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) ServerStateMachine {
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
	f.rf.resetElectionTimeout()

	return f
}

func (f *Follower) shouldRetryFailedRequestVote(_ *RequestVoteArgs) bool {
	return false
}

func (f *Follower) processRequestVoteResponse(serverId int, args *RequestVoteArgs, _ *RequestVoteReply) ServerStateMachine {
	DPrintf(f.rf.me, cmpFollower, "<~~~ S%d Stale Response to RequestVote(%v). Ignoring", serverId, args)
	return f
}

func (f *Follower) processIncomingAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) ServerStateMachine {
	// Check 1 - Section 5.1 of Raft paper
	if f.rf.currentTerm > args.Term {
		DPrintf(f.rf.me, cmpFollower, "AppendEntries from S%d@T%d < S%d@T%d. Replying false.",
			args.LeaderId, args.Term, f.rf.me, f.rf.currentTerm)
		reply.Term = f.rf.currentTerm
		reply.Success = false
		return f
	}

	// Check 2 - Section 5.3 of Raft paper
	if !f.rf.log.hasEntryAt(args.PrevLogIndex, args.PrevLogTerm) {
		DPrintf(f.rf.me, cmpFollower, "doesn't have log entries I%d@T%d. Replying false.",
			args.PrevLogIndex, args.PrevLogIndex)
		reply.Term = f.rf.currentTerm
		reply.Success = false
		return f
	}

	f.rf.log.insertReplicatedEntries(args.Entries)

	// Check 5 - Update Commit Index
	if f.rf.commitIndex < args.LeaderCommit {
		var maxEntry = 0
		if len(args.Entries) == 0 {
			maxEntry = args.PrevLogIndex
		} else {
			maxEntry = args.Entries[len(args.Entries)-1].Index
		}

		var newCommitIndex = max(maxEntry, args.LeaderCommit)

		DPrintf(f.rf.me, cmpFollower, "leaderCommit %d > commitIndex %d. updating to %d",
			args.LeaderCommit, f.rf.commitIndex, newCommitIndex)

		f.rf.commitIndex = newCommitIndex
		go f.rf.applyLog()
	}

	DPrintf(f.rf.me, cmpFollower, "AppendEntries from S%d@T%d. Replying true.", args.LeaderId, args.Term)
	reply.Term = f.rf.currentTerm
	reply.Success = true
	f.rf.resetElectionTimeout()

	return f
}

func (f *Follower) shouldRetryFailedAppendEntries(_ *AppendEntriesArgs) bool {
	return false
}

func (f *Follower) processAppendEntriesResponse(
	_ int,
	_ *AppendEntriesArgs,
	_ *AppendEntriesReply) ServerStateMachine {

	DPrintf(f.rf.me, cmpCandidate, "Received Stale AppendEntries Response. Ignoring.")
	return f
}

func (f *Follower) processCommand(command interface{}) (index int, term int) {
	panic("Follower should not be processing commands!")
}
