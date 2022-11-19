package raft

import "time"

type Leader struct {
	rf *Raft
}

func (l *Leader) isLeader() bool {
	return true
}

func (l *Leader) electionTimeoutElapsed() ServerState {
	l.rf.resetTimer()
	return l
}

func (l *Leader) requestVoteReceived(args *RequestVoteArgs, reply *RequestVoteReply) ServerState {
	DPrintf(l.rf.me, cmpLeader, "Denying RequestVote from C%d@T%d. Already Leader",
		args.CandidateId, args.Term)

	reply.Term = l.rf.currentTerm
	reply.VoteGranted = false

	return l
}

func (l *Leader) voteResponseReceived(serverId int, args *RequestVoteArgs, _ *RequestVoteReply) ServerState {
	DPrintf(l.rf.me, cmpLeader, "Ignoring RequestVote Response from S%d@T%d. Already Leader",
		serverId, args.Term)

	return l
}

func (l *Leader) appendEntriesReceived(args *AppendEntriesArgs, reply *AppendEntriesReply) ServerState {
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

func (l *Leader) shouldRetryRequestVote(_ *RequestVoteArgs) bool {
	return false
}

func (l *Leader) heartbeat() {
	// TODO: create a heartbeat id
	for l.rf.killed() == false {
		l.rf.mu.Lock()
		if l.rf.serverState != l {
			DPrintf(l.rf.me, cmpLeader, "No longer leader. Killing heartbeat.")
			l.rf.mu.Unlock()
			break
		}

		DPrintf(l.rf.me, cmpLeader, "Sending Heartbeat")
		for serverId := range l.rf.peers {
			if serverId == l.rf.me {
				continue
			}

			go func(rf *Raft, serverId, term int) {
				args := &AppendEntriesArgs{Term: term, LeaderId: rf.me}
				reply := &AppendEntriesReply{}

				ok := l.rf.sendAppendEntries(serverId, args, reply)
				if !ok {
					// TODO: Handle error
				}

				rf.mu.Lock()
				rf.serverState = rf.checkTerm(serverId, reply.Term)
				rf.mu.Unlock()
			}(l.rf, serverId, l.rf.currentTerm)
		}

		l.rf.mu.Unlock()
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}
