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

	// Check if candidate is at least as up to date (section 5.4)
	lastIndex, lastTerm := f.rf.log.lastLogEntry()
	if lastTerm > args.LastLogTerm || (lastTerm == args.LastLogTerm && lastIndex > args.LastLogIndex) {
		DPrintf(f.rf.me, cmpFollower, "candidate C%d last log %d@T%d < current server %d@T%d, rejecting",
			args.CandidateId, args.LastLogIndex, args.LastLogTerm, lastIndex, lastTerm)
		reply.Term = f.rf.currentTerm
		reply.VoteGranted = false
		return f
	}

	f.rf.votedFor = args.CandidateId
	f.rf.persist()
	DPrintf(f.rf.me, cmpFollower, "Granting vote to C%d @ T%d, Resetting Timer", args.CandidateId, args.Term)
	reply.Term = f.rf.currentTerm
	reply.VoteGranted = true
	f.rf.resetElectionTimeout()

	return f
}

func (f *Follower) processRequestVoteResponse(serverId ServerId, args *RequestVoteArgs, _ *RequestVoteReply) ServerStateMachine {
	DPrintf(f.rf.me, cmpFollower, "<~~~ S%d Stale Response to RequestVote(%v). Ignoring", serverId, args)
	return f
}

func (f *Follower) processIncomingAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) ServerStateMachine {
	reply.Term = f.rf.currentTerm

	// Check 1 - Is the AppendEntry from current leader Section 5.1 of Raft paper
	if f.rf.currentTerm > args.Term {
		DPrintf(f.rf.me, cmpFollower, "AppendEntries from S%d@T%d < S%d@T%d. Replying false.",
			args.LeaderId, args.Term, f.rf.me, f.rf.currentTerm)
		reply.Success = false
		return f
	}

	// Received AppendEntries from current leader, reset election timer
	defer f.rf.resetElectionTimeout()

	// Check 2 - Section 5.3 of Raft paper
	// Case 1 - log is shorter than prevLogIndex. In that case, tell leader where log next index is
	// Case 2 - entry at prevLogIndex conflicts, tell leader where start of term is
	logNextIndex := f.rf.log.nextIndex()
	if logNextIndex <= args.PrevLogIndex {
		DPrintf(f.rf.me, cmpFollower, "log is shorter than %d. Telling leader to rewind to %d.",
			args.PrevLogIndex, logNextIndex)
		reply.Success = false
		reply.RewindIndex = logNextIndex
		return f
	}

	hasPrevEntry, conflictTerm, conflictStartIndex := f.rf.log.hasEntryAt(args.PrevLogIndex, args.PrevLogTerm)
	if !hasPrevEntry {
		DPrintf(f.rf.me, cmpFollower, "log entry at %d has conflicting term T%d!=T%d. Replying false, but resetting election timeout.",
			args.PrevLogIndex, conflictTerm, args.PrevLogTerm)
		reply.Success = false
		reply.RewindIndex = conflictStartIndex
		return f
	}

	if len(args.Entries) > 0 {
		f.rf.log.insertReplicatedEntries(args.Entries)
	}

	if f.rf.commitIndex < args.LeaderCommit {
		f.updateCommitIndex(args)
	}

	DPrintf(f.rf.me, cmpFollower, "AppendEntries from S%d@T%d. Replying true and resetting election timeout", args.LeaderId, args.Term)
	reply.Success = true

	return f
}

func (f *Follower) updateCommitIndex(args *AppendEntriesArgs) {
	var maxEntry LogIndex = 0
	if len(args.Entries) == 0 {
		maxEntry = args.PrevLogIndex
	} else {
		maxEntry = args.Entries[len(args.Entries)-1].Index
	}

	var newCommitIndex = min(maxEntry, args.LeaderCommit)

	DPrintf(f.rf.me, cmpFollower, "leaderCommit %d > commitIndex %d. updating to %d",
		args.LeaderCommit, f.rf.commitIndex, newCommitIndex)

	f.rf.commitIndex = newCommitIndex
	go f.rf.applyLog()
}

func (f *Follower) processAppendEntriesResponse(
	_ ServerId,
	_ *AppendEntriesArgs,
	_ *AppendEntriesReply) ServerStateMachine {

	DPrintf(f.rf.me, cmpCandidate, "Received Stale AppendEntries Response. Ignoring.")
	return f
}

func (f *Follower) processCommand(_ interface{}) (index LogIndex, term Term) {
	panic("Follower should not be processing commands!")
}
