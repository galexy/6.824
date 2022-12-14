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

	p.raft.Debug("=-=> S%d Send RequestVote(%v)", p.serverId, args)
	if !p.endPoint.Call("Raft.RequestVote", args, reply) {
		p.raft.Debug("=/=> S%d Failed RequestVote()", p.serverId)
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

	p.raft.Debug("=-=> S%d Send AppendEntries(%v)", p.serverId, args)
	if !p.endPoint.Call("Raft.AppendEntries", args, reply) {
		p.raft.Debug("=/=> S%d Failed AppendEntries()", p.serverId)
		return
	}

	p.raft.dispatchAppendEntriesResponse(p, args, reply)
}

func (p *Peer) callInstallSnapshot(leaderId ServerId, term Term,
	lastIncludedIndex LogIndex, latIncludedTerm Term, data []byte) {

	args := &InstallSnapshotArgs{
		LeaderId:          leaderId,
		Term:              term,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  latIncludedTerm,
		Data:              data,
	}
	reply := &InstallSnapshotReply{}

	p.raft.Debug("=-=> S%d Send InstallSnapshot(%v)", p.serverId, args)
	if !p.endPoint.Call("Raft.InstallSnapshot", args, reply) {
		p.raft.Debug("=/=> S%d Failed InstallSnapshot()", p.serverId)
		return
	}

	p.raft.dispatchInstallSnapshotResponse(p, args, reply)
}
