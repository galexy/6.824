package raft

import (
	"6.824/labrpc"
)

type Peer struct {
	raft     *Raft
	endPoint *labrpc.ClientEnd
	serverId int
}

func (p *Peer) callRequestVote(term, candidate, lastLogIndex, lastLogTerm int) {
	args := &RequestVoteArgs{Term: term, CandidateId: candidate, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
	reply := &RequestVoteReply{}

	DPrintf(p.raft.me, cmpRPC, "=-=> S%d Send RequestVote(%v)", p.serverId, args)
	for !p.endPoint.Call("Raft.RequestVote", args, reply) {
		DPrintf(p.raft.me, cmpRPC, "=/=> S%d Failed RequestVote()", p.serverId)

		if p.raft.shouldRetryRequestVote(args) {
			DPrintf(p.raft.me, cmpRPC, "=~=> S%d Retrying RequestVote())", p.serverId)
		} else {
			DPrintf(p.raft.me, cmpRPC, "=/=> S%d Dropping RequestVote()", p)
			return
		}
	}

	p.raft.dispatchRequestVoteResponse(p, args, reply)
}

func (p *Peer) callAppendEntries(leaderId int, term int, prevLogIndex, prevLogTerm int, entries []*LogEntry, leaderCommit int) {
	args := &AppendEntriesArgs{
		LeaderId:     leaderId,
		Term:         term,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	reply := &AppendEntriesReply{}

	DPrintf(p.raft.me, cmpRPC, "=-=> S%d Send AppendEntries(%v)", p.serverId, args)
	for !p.endPoint.Call("Raft.AppendEntries", args, reply) {
		DPrintf(p.raft.me, cmpRPC, "=/=> S%d Failed AppendEntries()", p.serverId)

		if p.raft.shouldRetryFailedAppendEntries(args) {
			DPrintf(p.raft.me, cmpRPC, "=~=> S%d Retrying AppendEntries())", p.serverId)
		} else {
			DPrintf(p.raft.me, cmpRPC, "=/=> S%d Dropping AppendEntries()", p)
			return
		}
	}

	p.raft.dispatchAppendEntriesResponse(p, args, reply)
}
