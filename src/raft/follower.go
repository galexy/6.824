package raft

import (
	"6.824/logger"
	"fmt"
)

type Follower struct {
	rf *Raft
}

func (f *Follower) Debug(format string, a ...interface{}) {
	prefix := fmt.Sprintf("[S%d][%v] S%d ", f.rf.me, cmpFollower, f.rf.me)
	logger.DPrintf(prefix+format, a...)
}

func (f *Follower) isLeader() bool {
	return false
}

func (f *Follower) processTick() {
	panic("Followers only process election timeout")
}

func (f *Follower) processElectionTimeout() ServerStateMachine {
	f.Debug("Converting to Candidate")

	candidate := &Candidate{rf: f.rf}
	candidate.startElection()

	return candidate
}

func (f *Follower) processIncomingRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) ServerStateMachine {
	if f.rf.currentTerm > args.Term {
		f.Debug("@T%d > C%d@ T%d, Rejecting", f.rf.currentTerm, args.CandidateId, args.Term)
		reply.Term = f.rf.currentTerm
		reply.VoteGranted = false
		return f
	}

	if f.rf.votedFor != -1 {
		f.Debug("Already voted for S%d @ T%d", f.rf.votedFor, f.rf.currentTerm)
		reply.Term = f.rf.currentTerm
		reply.VoteGranted = false
		return f
	}

	// Check if candidate is at least as up to date (section 5.4)
	lastIndex, lastTerm := f.rf.log.lastLogEntry()
	if lastTerm > args.LastLogTerm || (lastTerm == args.LastLogTerm && lastIndex > args.LastLogIndex) {
		f.Debug("candidate C%d last log %d@T%d < current server %d@T%d, rejecting",
			args.CandidateId, args.LastLogIndex, args.LastLogTerm, lastIndex, lastTerm)
		reply.Term = f.rf.currentTerm
		reply.VoteGranted = false
		return f
	}

	f.rf.votedFor = args.CandidateId
	f.rf.persist()
	f.Debug("Granting vote to C%d @ T%d, Resetting Timer", args.CandidateId, args.Term)
	reply.Term = f.rf.currentTerm
	reply.VoteGranted = true
	f.rf.resetElectionTimeout()

	return f
}

func (f *Follower) processRequestVoteResponse(serverId ServerId, args *RequestVoteArgs, _ *RequestVoteReply) ServerStateMachine {
	f.Debug("<~~~ S%d Stale Response to RequestVote(%v). Ignoring", serverId, args)
	return f
}

func (f *Follower) processIncomingAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) ServerStateMachine {
	reply.Term = f.rf.currentTerm

	// Check 1 - Is the AppendEntry from current leader Section 5.1 of Raft paper
	if f.rf.currentTerm > args.Term {
		f.Debug("AppendEntries from S%d@T%d < S%d@T%d. Replying false.",
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
		f.Debug("log is shorter than %d. Telling leader to rewind to %d.",
			args.PrevLogIndex, logNextIndex)
		reply.Success = false
		reply.RewindIndex = logNextIndex
		return f
	}

	hasPrevEntry, conflictTerm, conflictStartIndex := f.rf.log.hasEntryAt(args.PrevLogIndex, args.PrevLogTerm)
	if !hasPrevEntry {
		f.Debug("log entry at %d has conflicting term T%d!=T%d. Replying false, but resetting election timeout.",
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

	f.Debug("AppendEntries from S%d@T%d. Replying true and resetting election timeout", args.LeaderId, args.Term)
	reply.Success = true

	return f
}

func (f *Follower) processIncomingInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) ServerStateMachine {
	reply.Term = f.rf.currentTerm

	if f.rf.currentTerm > args.Term {
		f.Debug("AppendEntries from S%d@T%d < S%d@T%d. Not processing.",
			args.Term, f.rf.currentTerm)
		return f
	}

	hasPrevEntry, _, _ := f.rf.log.hasEntryAt(args.LastIncludedIndex, args.LastIncludedTerm)

	if hasPrevEntry {
		f.Debug("Found LogEntry(I=%d, T=%d).",
			args.LastIncludedIndex, args.LastIncludedTerm)
		compactToIndex := args.LastIncludedIndex
		if f.rf.lastApplied < compactToIndex {
			f.Debug("LastApplied(%d) < LastIncludedIndex(%d).",
				f.rf.lastApplied, args.LastIncludedIndex)
			compactToIndex = f.rf.lastApplied + 1
		}

		f.Debug("Compacting log to %d", compactToIndex)
		f.rf.log.compactAt(compactToIndex)
		f.rf.persist()
		return f
	}

	f.Debug("Not Found LogEntry(I=%d, T=%d). Discarding Log. Updating lastApplied=%d",
		args.LastIncludedIndex, args.LastIncludedTerm, args.LastIncludedIndex)

	// Update lastApplied, but leave commitIndex alone. It will be updated on next heartbeat
	f.rf.lastApplied = args.LastIncludedIndex
	f.rf.log.discard(args.LastIncludedIndex, args.LastIncludedTerm)
	f.rf.snapshot = args.Data
	f.rf.persist()

	msg := &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: int(args.LastIncludedIndex),
		SnapshotTerm:  int(args.LastIncludedTerm),
	}

	f.rf.applyChn <- *msg

	return f
}

func (f *Follower) processInstallSnapshotResponse(serverId ServerId, args *InstallSnapshotArgs, reply *InstallSnapshotReply) ServerStateMachine {
	f.Debug("<~~~ S%d Stale Response to InstallSnapshot(%v). Ignoring", serverId, args)
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

	f.Debug("leaderCommit %d > commitIndex %d. updating to %d",
		args.LeaderCommit, f.rf.commitIndex, newCommitIndex)

	f.rf.commitIndex = newCommitIndex
	go f.rf.applyLog()
}

func (f *Follower) processAppendEntriesResponse(
	_ ServerId,
	_ *AppendEntriesArgs,
	_ *AppendEntriesReply) ServerStateMachine {

	f.Debug("Received Stale AppendEntries Response. Ignoring.")
	return f
}

func (f *Follower) processCommand(_ interface{}) (index LogIndex, term Term) {
	panic("Follower should not be processing commands!")
}
