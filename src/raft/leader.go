package raft

import (
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

type Leader struct {
	rf            *Raft       // Reference to main raft
	nextIndex     []LogIndex  // for each peer, Index of the next log entry to send that server
	maxIndex      []LogIndex  // for each server Index of the highest known replicated log entry
	nextHeartbeat []time.Time // For each peer, when will next heartbeat expire
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
			prevEntry, entries := l.rf.log.getEntriesFrom(l.nextIndex[peerId])

			// reset heartbeat timeout for peer
			l.nextHeartbeat[peerId] = now.Add(l.rf.heartBeatInterval)

			// send AppendEntries
			go l.rf.peers[peerId].callAppendEntries(
				l.rf.me,
				l.rf.currentTerm,
				prevEntry.Index,
				prevEntry.Term,
				entries,
				l.rf.commitIndex)
		}
	}
}

func (l *Leader) processElectionTimeout() ServerStateMachine {
	panic("Leaders should not see election timeouts")
}

func (l *Leader) processIncomingRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) ServerStateMachine {
	DPrintf(l.rf.me, cmpLeader, "Denying RequestVote(C=%d,T=%d). Already Leader",
		args.CandidateId, args.Term)

	reply.Term = l.rf.currentTerm
	reply.VoteGranted = false

	return l
}

func (l *Leader) processRequestVoteResponse(serverId ServerId, args *RequestVoteArgs, _ *RequestVoteReply) ServerStateMachine {
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

	DPrintf(l.rf.me, cmpLeader, "S%d ~~> S%d AppendEntriesReply(T=%d,S=false)",
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
			DPrintf(l.rf.me, cmpLeader, "decrementing nextIndex(S=%d) to %d", serverId, nextIndex-1)
			l.nextIndex[serverId] = nextIndex - 1
		} else {
			DPrintf(l.rf.me, cmpLeader, "jumping nextIndex(S=%d) back to %d", serverId, reply.RewindIndex)
			l.nextIndex[serverId] = reply.RewindIndex
		}

		return l
	}

	// if success, update nextIndex and maxIndex for follower (section 5.3)
	l.updateIndexes(serverId, args)

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
		DPrintf(l.rf.me, cmpLeader, "nextIndex(S=%d) %d >= %d, not updating",
			serverId, currentNextIndex, newNextIndex)
	} else {
		DPrintf(l.rf.me, cmpLeader, "update nextIndex(S=%d) to %d.", serverId, newNextIndex)
		l.nextIndex[serverId] = newNextIndex
	}

	maxUpdated := false
	if currentMaxIndex >= newMaxIndex {
		DPrintf(l.rf.me, cmpLeader, "maxIndex(S=%d) %d >= %d, not updating",
			serverId, currentMaxIndex, newMaxIndex)
	} else {
		DPrintf(l.rf.me, cmpLeader, "update maxIndex(S=%d) to %d.", serverId, newMaxIndex)
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
		DPrintf(l.rf.me, cmpLeader, "%d nodes have commitIndex=>%d", accum, index)
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
				DPrintf(l.rf.me, cmpLeader, "updating commitIndex=%d.", index)
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
	DPrintf(l.rf.me, cmpLeader, "enqueue(Command=%v,I=%d,T%d)",
		command, newEntry.Index, newEntry.Term)
	return newEntry.Index, newEntry.Term
}
