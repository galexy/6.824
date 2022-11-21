package raft

import (
	"fmt"
	"time"
)

type AppendEntriesArgs struct {
	Term         int // Leader's Term
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

func (a *AppendEntriesArgs) String() string {
	return fmt.Sprintf("L%d, T%d, PI:%d, PT:%d, LC:%d, %v",
		a.LeaderId, a.Term, a.PrevLogIndex, a.PrevLogTerm, a.LeaderCommit, a.Entries)
}

func (a *AppendEntriesArgs) isHeartbeat() bool {
	return len(a.Entries) == 0
}

type AppendEntriesReply struct {
	Term    int // currentTerm, for leader to update itself
	Success bool
}

func (r *AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, %v", r.Term, r.Success)
}

type Leader struct {
	rf            *Raft       // Reference to main raft
	nextIndex     []int       // for each peer, Index of the next log entry to send that server
	maxIndex      []int       // for each server Index of the highest known replicated log entry
	nextHeartbeat []time.Time // For each peer, when will next heartbeat expire
}

func MakeLeader(rf *Raft) *Leader {
	leader := &Leader{rf: rf}
	leader.nextIndex = make([]int, len(rf.peers))
	nextIndex := rf.log.nextIndex()
	for i, _ := range leader.nextIndex {
		leader.nextIndex[i] = nextIndex
	}
	leader.maxIndex = make([]int, len(rf.peers))
	for i, _ := range leader.maxIndex {
		leader.maxIndex[i] = -1
	}
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
		if peerId == l.rf.me {
			continue
		}

		if now.After(beat) || now.Equal(beat) {
			// TODO: add support for pure heartbeat
			prevEntry, entries := l.rf.log.getEntriesFrom(l.nextIndex[peerId])

			// reset heartbeat timeout for peer
			l.nextHeartbeat[peerId] = now.Add(l.rf.heartBeatInterval)

			// send AppendEntries
			prevLogIndex := -1
			prevLogTerm := -1
			if prevEntry != nil {
				prevLogIndex = prevEntry.Index
				prevLogTerm = prevEntry.Term
			}
			go l.rf.peers[peerId].callAppendEntries(
				l.rf.me,
				l.rf.currentTerm,
				prevLogIndex,
				prevLogTerm,
				entries,
				l.rf.commitIndex)
		}
	}
}

func (l *Leader) processElectionTimeout() ServerStateMachine {
	panic("Leaders should not see election timeouts")
}

func (l *Leader) processIncomingRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) ServerStateMachine {
	DPrintf(l.rf.me, cmpLeader, "Denying RequestVote from C%d@T%d. Already Leader",
		args.CandidateId, args.Term)

	reply.Term = l.rf.currentTerm
	reply.VoteGranted = false

	return l
}

func (l *Leader) processRequestVoteResponse(serverId int, args *RequestVoteArgs, _ *RequestVoteReply) ServerStateMachine {
	DPrintf(l.rf.me, cmpLeader, "Ignoring RequestVote Response from S%d@T%d. Already Leader",
		serverId, args.Term)

	return l
}

func (l *Leader) processIncomingAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) ServerStateMachine {
	if args.Term >= l.rf.currentTerm {
		// 1) Shouldn't have a leader with the same Term
		// 2) Newer Term should have made current server a follower
		panic("Leader unexpectedly received AppendEntries for current or newer Term.")
	}

	DPrintf(l.rf.me, cmpLeader, "S%d ~~> S%d AppendEntries Resp (T:%d, Success:false)",
		l.rf.me, args.LeaderId, l.rf.currentTerm)
	reply.Term = l.rf.currentTerm
	reply.Success = false

	return l
}

func (l *Leader) shouldRetryFailedRequestVote(_ *RequestVoteArgs) bool {
	return false
}

func (l *Leader) shouldRetryFailedAppendEntries(args *AppendEntriesArgs) bool {
	return args.Term == l.rf.currentTerm && !args.isHeartbeat()
}

func (l *Leader) processAppendEntriesResponse(
	serverId int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply) ServerStateMachine {

	// TODO: add check for failure due to outdated term

	// TODO: add check for log inconsitency

	// if success, update nextIndex and maxIndex for follower (section 5.3)
	l.updateIndexes(serverId, args)

	return l
}

func (l *Leader) updateIndexes(serverId int, args *AppendEntriesArgs) {
	currentNextIndex := l.nextIndex[serverId]
	currentMaxIndex := l.maxIndex[serverId]

	var newNextIndex, newMaxIndex int
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
		DPrintf(l.rf.me, cmpLeader, "nextIndex for S%d %d >= %d, not updating",
			serverId, currentNextIndex, newNextIndex)
	} else {
		DPrintf(l.rf.me, cmpLeader, "update nextIndex for S%d to %d.", serverId, newNextIndex)
		l.nextIndex[serverId] = newNextIndex
	}

	maxUpdated := false
	if currentMaxIndex >= newMaxIndex {
		DPrintf(l.rf.me, cmpLeader, "maxIndex for S%d %d >= %d, not updating",
			serverId, currentMaxIndex, newMaxIndex)
	} else {
		DPrintf(l.rf.me, cmpLeader, "update maxIndex for S%d to %d.", serverId, newMaxIndex)
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
	DPrintf(l.rf.me, cmpLeader, "checking if commitIndex needs to be updated")
	start := l.rf.commitIndex
	end := l.rf.log.nextIndex()

	indexCounts := make([]int, end-start)
	for _, max := range l.maxIndex {
		if max == -1 {
			continue
		}
		indexCounts[max-start]++
	}

	majority := len(l.rf.peers)/2 + 1
	accum := 1
	for index := end - 1; index >= start; index-- {
		accum += indexCounts[index-l.rf.commitIndex]
		DPrintf(l.rf.me, cmpLeader, "%d nodes have commitIndex %d or higher", accum, index)
		if accum >= majority {
			maxCommitEntry := l.rf.log.getEntryAt(index)
			if maxCommitEntry == nil {
				return false
			}

			commitTerm := maxCommitEntry.Term
			if commitTerm != l.rf.currentTerm {
				DPrintf(l.rf.me, cmpLeader, "max majority commit index not current term (%d@T%d)", index, commitTerm)
				return false
			}

			if l.rf.commitIndex < index {
				DPrintf(l.rf.me, cmpLeader, "updating commitIndex to %d.", index)
				l.rf.commitIndex = index
				return true
			}

			return false
		}
	}

	return false
}

func (l *Leader) processCommand(command interface{}) (index int, term int) {
	newEntry, _ := l.rf.log.append(l.rf.currentTerm, command)
	DPrintf(l.rf.me, cmpLeader, "enqueue(Command=%v) @(%d, T%d)",
		command, newEntry.Index, newEntry.Term)
	return newEntry.Index, newEntry.Term
}
