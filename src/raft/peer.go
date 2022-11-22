package raft

import (
	"6.824/labrpc"
)

type Peer struct {
	raft     *Raft
	endPoint *labrpc.ClientEnd
	serverId ServerId
}

func (p *Peer) callRequestVote(term Term, candidate ServerId, lastLogIndex LogIndex, lastLogTerm Term) {
	args := &RequestVoteArgs{Term: term, CandidateId: candidate, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}
	reply := &RequestVoteReply{}

	DPrintf(p.raft.me, cmpRPC, "=-=> S%d Send RequestVote(%v)", p.serverId, args)
	if !p.endPoint.Call("Raft.RequestVote", args, reply) {
		DPrintf(p.raft.me, cmpRPC, "=/=> S%d Failed RequestVote()", p.serverId)
		return
	}

	p.raft.dispatchRequestVoteResponse(p, args, reply)
}

func (p *Peer) callAppendEntries(leaderId ServerId, term Term, prevLogIndex LogIndex,
	prevLogTerm Term, entries []*LogEntry, leaderCommit LogIndex) {

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
	if !p.endPoint.Call("Raft.AppendEntries", args, reply) {
		DPrintf(p.raft.me, cmpRPC, "=/=> S%d Failed AppendEntries()", p.serverId)
		return
	}

	p.raft.dispatchAppendEntriesResponse(p, args, reply)
}
