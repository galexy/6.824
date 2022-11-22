package raft

import (
	"fmt"
)

type LogEntry struct {
	Index   LogIndex
	Term    Term
	Command interface{}
}

func (l *LogEntry) String() string {
	return fmt.Sprintf("%d@T%d %v", l.Index, l.Term, l.Command)
}

type Log interface {
	append(term Term, command interface{}) (newEntry, prevEntry *LogEntry)
	hasEntryAt(index LogIndex, term Term) (hasPrevEntry bool, conflictTerm Term, conflictTermStartIndex LogIndex)
	getEntriesFrom(index LogIndex) (prevEntry *LogEntry, entries []*LogEntry)
	nextIndex() LogIndex
	insertReplicatedEntries(entries []*LogEntry)
	lastLogEntry() (index LogIndex, term Term)
	getEntryAt(index LogIndex) (entry *LogEntry)
}

type LogImpl struct {
	entries []*LogEntry // Entries in the current server's log
	rf      *Raft       // Link to main raft
}

func MakeLog(rf *Raft) Log {
	log := &LogImpl{rf: rf}
	log.entries = make([]*LogEntry, 1, 1024)
	log.entries[0] = &LogEntry{Index: 0, Term: 0, Command: nil}

	return log
}

func (l *LogImpl) append(term Term, command interface{}) (newEntry, prevEntry *LogEntry) {
	newIndex := l.nextIndex()
	if newIndex > 0 {
		prevEntry = l.entries[newIndex-1]
	}
	newEntry = &LogEntry{Index: newIndex, Term: term, Command: command}
	l.entries = append(l.entries, newEntry)

	return
}

func (l *LogImpl) hasEntryAt(index LogIndex, term Term) (hasEntry bool, conflictTerm Term, conflictTermStartIndex LogIndex) {
	//if index == 0 && term == 0 {
	//	return true, 0, 0
	//}

	if len(l.entries) <= int(index) {
		return
	}

	entryAtIndex := l.entries[index]
	if entryAtIndex.Term == term {
		hasEntry = true
		return
	}

	// Find the first index of the conflicting term
	// TODO: build an map to find these index positions in O(1) time
	startIndex := index
	for ; startIndex > 0 && l.entries[startIndex].Term == entryAtIndex.Term; startIndex-- {
	}
	startIndex = startIndex + 1

	return false, entryAtIndex.Term, startIndex
}

func (l *LogImpl) getEntriesFrom(index LogIndex) (prevEntry *LogEntry, entries []*LogEntry) {
	prevIndex := index - 1
	if prevIndex >= 0 {
		prevEntry = l.entries[prevIndex]
	}

	if int(index) < len(l.entries) {
		entries = l.entries[index:]
	}

	return
}

func (l *LogImpl) nextIndex() LogIndex {
	return LogIndex(len(l.entries))
}

func (l *LogImpl) insertReplicatedEntries(entries []*LogEntry) {
	for _, entry := range entries {
		nextIndex := l.nextIndex()
		if entry.Index > nextIndex {
			DPrintf(l.rf.me, cmpLogger, "Unexpected replicated entry @%d, expecting %d.", entry.Index, nextIndex)
			panic(fmt.Sprintf("Unexpected replicated entry @%d, expecting %d.", entry.Index, nextIndex))
		}

		// case where new entry is appended to end of log
		if entry.Index == nextIndex {
			// Append new entry
			DPrintf(l.rf.me, cmpLogger, "Appending replicated entry %d@T%d", entry.Index, entry.Term)
			l.entries = append(l.entries, entry)
			continue
		}

		// entry index already in log, check if there is a conflict
		existingEntry := l.entries[entry.Index]
		if existingEntry.Term != entry.Term {
			DPrintf(l.rf.me, cmpLogger, "Conflict detected %d@T%d != replicated entry %d@T%d. Deleting entry and all that follow.",
				existingEntry.Index, existingEntry.Term, entry.Index, entry.Term)
			// TODO: add truncation to persistence layer
			l.entries = l.entries[:entry.Index]
			l.entries = append(l.entries, entry)
		}

		DPrintf(l.rf.me, cmpLogger, "Already have %d@T%d, skipping.", entry.Index, entry.Term)
	}
}

func (l *LogImpl) lastLogEntry() (index LogIndex, term Term) {
	lastEntry := l.entries[len(l.entries)-1]
	return lastEntry.Index, lastEntry.Term
}

func (l *LogImpl) getEntryAt(index LogIndex) (entry *LogEntry) {
	return l.entries[index]
}
