package raft

import "fmt"

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

func (l *LogEntry) String() string {
	return fmt.Sprintf("%d@T%d %v", l.Index, l.Term, l.Command)
}

type Log interface {
	append(term int, command interface{}) (newEntry, prevEntry *LogEntry)
	hasEntryAt(index int, term int) bool
	getEntriesFrom(index int) (prevEntry *LogEntry, entries []*LogEntry)
	nextIndex() int
	insertReplicatedEntries(entries []*LogEntry)
	getEntryAt(index int) (entry *LogEntry)
}

type LogImpl struct {
	entries []*LogEntry // Entries in the current server's log
	rf      *Raft       // Link to main raft
}

func MakeLog(rf *Raft) Log {
	log := &LogImpl{rf: rf}
	log.entries = make([]*LogEntry, 0, 1024)

	return log
}

func (l *LogImpl) append(term int, command interface{}) (newEntry, prevEntry *LogEntry) {
	newIndex := len(l.entries)
	if newIndex > 0 {
		prevEntry = l.entries[newIndex-1]
	}
	newEntry = &LogEntry{Index: newIndex, Term: term, Command: command}
	l.entries = append(l.entries, newEntry)

	return
}

func (l *LogImpl) hasEntryAt(index int, term int) bool {
	if index == -1 && term == -1 {
		return true
	}

	return index >= 0 && len(l.entries) > index && l.entries[index].Term == term
}

func (l *LogImpl) getEntriesFrom(index int) (prevEntry *LogEntry, entries []*LogEntry) {
	prevIndex := index - 1
	if prevIndex >= 0 {
		prevEntry = l.entries[prevIndex]
	}

	if index < len(l.entries) {
		entries = l.entries[index:]
	}

	return
}

func (l *LogImpl) nextIndex() int {
	return len(l.entries)
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

func (l *LogImpl) getEntryAt(index int) (entry *LogEntry) {
	if index == -1 {
		return nil
	}

	return l.entries[index]
}
