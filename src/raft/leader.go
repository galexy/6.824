package raft

import (
	"6.824/logger"
	"fmt"
	"time"
)

type AppendEntriesArgs struct {
	Term         Term // Leader's Term
	LeaderId     ServerId
	PrevLogIndex LogIndex
	PrevLogTerm  Term
	Entries      []*LogEntry
	LeaderCommit LogIndex
}

func (a *AppendEntriesArgs) String() string {
	var entries = ""
	if len(a.Entries) <= 3 {
		entries = fmt.Sprintf("%v", a.Entries)
	} else {
		entries = fmt.Sprintf("[%v...%v]", a.Entries[0], a.Entries[len(a.Entries)-1])
	}

	return fmt.Sprintf("L=%d, T=%d, PI=%d, PT=%d, LC=%d, c=%v",
		a.LeaderId, a.Term, a.PrevLogIndex, a.PrevLogTerm, a.LeaderCommit, entries)
}

func (a *AppendEntriesArgs) isHeartbeat() bool {
	return len(a.Entries) == 0
}

type AppendEntriesReply struct {
	Term    Term // currentTerm, for leader to update itself
	Success bool
	//	ConflictTerm       Term
	RewindIndex LogIndex
}

func (r *AppendEntriesReply) String() string {
	return fmt.Sprintf("T=%d, S=%v", r.Term, r.Success)
}

type InstallSnapshotArgs struct {
	Term              Term
	LeaderId          ServerId
	LastIncludedIndex LogIndex
	LastIncludedTerm  Term
	Data              []byte
}

func (a *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("T=%d, L=%d, LI=%d, LT=%d, D=len(%d)",
		a.Term, a.LeaderId, a.LastIncludedIndex, a.LastIncludedTerm, len(a.Data))
}

type InstallSnapshotReply struct {
	Term Term
}

func (r *InstallSnapshotReply) String() string {
	return fmt.Sprintf("T=%d", r.Term)
}

type Leader struct {
	rf            *Raft       // Reference to main raft
	nextIndex     []LogIndex  // for each peer, Index of the next log entry to send that server
	maxIndex      []LogIndex  // for each server Index of the highest known replicated log entry
	nextHeartbeat []time.Time // For each peer, when will next heartbeat expire
}

func (l *Leader) Debug(format string, a ...interface{}) {
	prefix := fmt.Sprintf("[S%d][%v] S%d ", l.rf.me, cmpLeader, l.rf.me)
	logger.DPrintf(prefix+format, a...)
}

func MakeLeader(rf *Raft) *Leader {
	leader := &Leader{rf: rf}
	leader.nextIndex = make([]LogIndex, len(rf.peers))
	nextIndex := rf.log.nextIndex()
	for index := range leader.nextIndex {
		leader.nextIndex[index] = nextIndex
	}
	leader.maxIndex = make([]LogIndex, len(rf.peers))
	leader.initHeartbeats()

	return leader
}

func (l *Leader) initHeartbeats() {
	now := time.Now()
	l.nextHeartbeat = make([]time.Time, len(l.rf.peers))
	for i := range l.nextHeartbeat {
		l.nextHeartbeat[i] = now
	}
	l.processTick()
}

func (l *Leader) isLeader() bool {
	return true
}

func (l *Leader) processTick() {
	now := time.Now()

	for peerId, beat := range l.nextHeartbeat {
		peerId := ServerId(peerId)

		if peerId == l.rf.me {
			continue
		}

		if now.After(beat) || now.Equal(beat) {
			dropped, prevEntry, entries := l.rf.log.getEntriesFrom(l.nextIndex[peerId])

			if dropped {
				// send snapshot to follower
				go l.rf.peers[peerId].callInstallSnapshot(
					l.rf.me,
					l.rf.currentTerm,
					prevEntry.Index,
					prevEntry.Term,
					l.rf.snapshot)
			} else {
				// send AppendEntries
				go l.rf.peers[peerId].callAppendEntries(
					l.rf.me,
					l.rf.currentTerm,
					prevEntry.Index,
					prevEntry.Term,
					entries,
					l.rf.commitIndex)
			}

			// reset heartbeat timeout for peer
			l.nextHeartbeat[peerId] = now.Add(l.rf.heartBeatInterval)

		}
	}
}

func (l *Leader) processElectionTimeout() ServerStateMachine {
	panic("Leaders should not see election timeouts")
}

func (l *Leader) processIncomingRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) ServerStateMachine {
	l.Debug("Denying RequestVote(C=%d,T=%d). Already Leader",
		args.CandidateId, args.Term)

	reply.Term = l.rf.currentTerm
	reply.VoteGranted = false

	return l
}

func (l *Leader) processRequestVoteResponse(serverId ServerId, args *RequestVoteArgs, _ *RequestVoteReply) ServerStateMachine {
	l.Debug("Ignoring RequestVote Response from S%d@T%d. Already Leader",
		serverId, args.Term)

	return l
}

func (l *Leader) processIncomingAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) ServerStateMachine {
	if args.Term >= l.rf.currentTerm {
		// 1) Shouldn't have a leader with the same Term
		// 2) Newer Term should have made current server a follower
		panic("Leader unexpectedly received AppendEntries for current or newer Term.")
	}

	l.Debug("S%d ~~> S%d AppendEntriesReply(T=%d,S=false)",
		l.rf.me, args.LeaderId, l.rf.currentTerm)
	reply.Term = l.rf.currentTerm
	reply.Success = false

	return l
}

func (l *Leader) processAppendEntriesResponse(
	serverId ServerId,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply) ServerStateMachine {

	// TODO: add check for failure due to outdated term

	// check for log inconsistency
	if !reply.Success && reply.Term == l.rf.currentTerm {
		if reply.RewindIndex == 0 {
			nextIndex := l.nextIndex[serverId]
			l.Debug("decrementing nextIndex(S=%d) to %d", serverId, nextIndex-1)
			l.nextIndex[serverId] = nextIndex - 1
		} else {
			l.Debug("jumping nextIndex(S=%d) back to %d", serverId, reply.RewindIndex)
			l.nextIndex[serverId] = reply.RewindIndex
		}

		return l
	}

	// if success, update nextIndex and maxIndex for follower (section 5.3)
	l.updateIndexes(serverId, args)

	return l
}

func (l *Leader) processIncomingInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) ServerStateMachine {
	if l.rf.currentTerm > args.Term {
		l.Debug("Received stale InstallSnapshot(S=%d, T=%d). Ignoring.", args.LeaderId, args.Term)
		reply.Term = l.rf.currentTerm
		return l
	}

	panic("Unexpected incoming InstallSnapshot()")
}

func (l *Leader) processInstallSnapshotResponse(serverId ServerId, args *InstallSnapshotArgs, reply *InstallSnapshotReply) ServerStateMachine {
	if l.rf.currentTerm != reply.Term {
		l.Debug("stale response InstallSnapshot(T=%d) -> T=%d. Ignoring.", args.Term, reply.Term)
		return l
	}

	currentNextIndex := l.nextIndex[serverId]
	currentMaxIndex := l.maxIndex[serverId]
	newNextIndex := args.LastIncludedIndex + 1
	newMaxIndex := args.LastIncludedIndex

	l.Debug("Snapshot successfully sent to S=%d. Updating nextIndex(from=%d, to=%d) and matchIndex(from=%d, to=%d)",
		serverId, currentNextIndex, newNextIndex, currentMaxIndex, newMaxIndex)
	l.nextIndex[serverId] = newNextIndex
	l.maxIndex[serverId] = newMaxIndex

	return l
}

func (l *Leader) updateIndexes(serverId ServerId, args *AppendEntriesArgs) {
	currentNextIndex := l.nextIndex[serverId]
	currentMaxIndex := l.maxIndex[serverId]

	var newNextIndex, newMaxIndex LogIndex
	if args.isHeartbeat() {
		// with a heart, the follower only confirms prevLogIndex
		newNextIndex = args.PrevLogIndex + 1
		newMaxIndex = args.PrevLogIndex
	} else {
		lastEntry := args.Entries[len(args.Entries)-1]
		newNextIndex = lastEntry.Index + 1
		newMaxIndex = lastEntry.Index
	}

	if currentNextIndex >= newNextIndex {
		l.Debug("nextIndex(S=%d) %d >= %d, not updating",
			serverId, currentNextIndex, newNextIndex)
	} else {
		l.Debug("update nextIndex(S=%d) to %d.", serverId, newNextIndex)
		l.nextIndex[serverId] = newNextIndex
	}

	maxUpdated := false
	if currentMaxIndex >= newMaxIndex {
		l.Debug("maxIndex(S=%d) %d >= %d, not updating",
			serverId, currentMaxIndex, newMaxIndex)
	} else {
		l.Debug("update maxIndex(S=%d) to %d.", serverId, newMaxIndex)
		l.maxIndex[serverId] = newMaxIndex
		maxUpdated = true
	}

	commitUpdated := false
	if maxUpdated {
		commitUpdated = l.updateCommitIndex()
	}

	if commitUpdated {
		go l.rf.applyLog()
	}

	return
}

func (l *Leader) updateCommitIndex() (updated bool) {
	l.Debug("checking if commitIndex needs to be updated")
	start := l.rf.commitIndex
	end := l.rf.log.nextIndex()

	indexCounts := make([]int, end-start)
	for _, max := range l.maxIndex {
		// if the max index for a peer is below leader commit index, it can't contribute
		// to a majority that would update commit index, so it's safe to skip
		if start > max {
			continue
		}
		indexCounts[max-start]++
	}

	majority := len(l.rf.peers)/2 + 1
	accum := 1
	for index := end - 1; index >= start; index-- {
		accum += indexCounts[index-l.rf.commitIndex]
		l.Debug("%d nodes have commitIndex=>%d", accum, index)
		if accum >= majority {
			maxCommitEntry := l.rf.log.getEntryAt(index)
			if maxCommitEntry == nil {
				return false
			}

			commitTerm := maxCommitEntry.Term
			if commitTerm != l.rf.currentTerm {
				l.Debug("max majority commit index not current term (%d@T%d)", index, commitTerm)
				return false
			}

			if l.rf.commitIndex < index {
				l.Debug("updating commitIndex=%d.", index)
				l.rf.commitIndex = index
				return true
			}

			return false
		}
	}

	return false
}

func (l *Leader) processCommand(command interface{}) (index LogIndex, term Term) {
	newEntry, _ := l.rf.log.append(l.rf.currentTerm, command)
	l.Debug("enqueue(Command=%v,I=%d,T%d)", command, newEntry.Index, newEntry.Term)

	now := time.Now()
	for i, _ := range l.nextHeartbeat {
		l.nextHeartbeat[i] = now
	}

	return newEntry.Index, newEntry.Term
}
