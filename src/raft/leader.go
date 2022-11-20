package raft

import (
	"fmt"
	"time"
)

type AppendEntriesArgs struct {
	Term     int // Leader's term
	LeaderId int
}

func (a *AppendEntriesArgs) String() string {
	return fmt.Sprintf("L%d, T%d", a.LeaderId, a.Term)
}

func (a *AppendEntriesArgs) isHeartbeat() bool {
	// TODO: update based on log entries
	return true
}

type AppendEntriesReply struct {
	Term    int // currentTerm, for leader to update itself
	Success bool
}

func (r *AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, %v", r.Term, r.Success)
}

type Leader struct {
	rf            *Raft
	nextHeartbeat []time.Time // For each peer, when will next heartbeat expire
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
			l.nextHeartbeat[peerId] = now.Add(l.rf.heartBeatInterval)
			go l.rf.peers[peerId].callAppendEntries(l.rf.me, l.rf.currentTerm)
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
		// 1) Shouldn't have a leader with the same term
		// 2) Newer term should have made current server a follower
		panic("Leader unexpectedly received AppendEntries for current or newer term.")
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

//func (l *Leader) heartbeat() {
//	for l.rf.killed() == false {
//		l.rf.mu.Lock()
//		if l.rf.serverStateMachine != l {
//			DPrintf(l.rf.me, cmpLeader, "No longer leader. Killing heartbeat.")
//			l.rf.mu.Unlock()
//			break
//		}
//
//		DPrintf(l.rf.me, cmpLeader, "Sending Heartbeat")
//		l.sendAppendEntries()
//
//		l.rf.mu.Unlock()
//		time.Sleep(time.Duration(100) * time.Millisecond)
//	}
//}

func (l *Leader) shouldRetryFailedAppendEntries(args *AppendEntriesArgs) bool {
	return args.Term == l.rf.currentTerm && !args.isHeartbeat()
}

func (l *Leader) processAppendEntriesResponse(
	serverId int,
	args *AppendEntriesArgs,
	reply *AppendEntriesReply) ServerStateMachine {

	return l
}

func (l *Leader) processCommand(command interface{}) (index int, term int) {
	panic("Candidate should not be processing commands!")
}

//func (l *Leader) sendAppendEntries() {
//	for peerId, peer := range l.rf.peers {
//		if peerId == l.rf.me {
//			continue
//		}
//
//		go peer.callAppendEntries(l.rf.me, l.rf.currentTerm)
//	}
//}
