package raft

import (
	"6.824/labgob"
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
	getEntryAt(index LogIndex) (entry *LogEntry)
	getEntriesFrom(index LogIndex) (prevEntry *LogEntry, entries []*LogEntry)
	nextIndex() LogIndex
	insertReplicatedEntries(entries []*LogEntry)
	lastLogEntry() (index LogIndex, term Term)
	save(encoder *labgob.LabEncoder) error
	load(encoder *labgob.LabDecoder) error
}

type LogImpl struct {
	startIndex LogIndex    // What is the latest index in the snapshot. This index represents 0 in slide
	entries    []*LogEntry // Slice holding active log entries, 1-based indexing with 0 holds sentinel
	rf         *Raft       // Link to main raft
}

func MakeLog(rf *Raft) Log {
	log := &LogImpl{rf: rf}
	log.startIndex = LogIndex(0)
	log.entries = make([]*LogEntry, 1, 1024)
	log.entries[0] = &LogEntry{Index: 0, Term: 0, Command: nil}

	return log
}

func (l *LogImpl) append(term Term, command interface{}) (newEntry, prevEntry *LogEntry) {
	newIndex := l.nextIndex()
	if newIndex <= 0 {
		panic("New index should be at least 1.")
	}

	prevEntry = l.getEntryAt(newIndex - 1)
	newEntry = &LogEntry{Index: newIndex, Term: term, Command: command}
	l.entries = append(l.entries, newEntry)
	l.rf.persist()

	return
}

func (l *LogImpl) hasEntryAt(index LogIndex, term Term) (hasEntry bool, conflictTerm Term, conflictTermStartIndex LogIndex) {
	if l.startIndex+LogIndex(len(l.entries)) <= index {
		panic("Not expected")
	}

	entryAtIndex := l.getEntryAt(index)
	if entryAtIndex.Term == term {
		hasEntry = true
		return
	}

	// Find the first index of the conflicting term
	startIndex := l.findTermStart(entryAtIndex)

	return false, entryAtIndex.Term, startIndex
}

func (l *LogImpl) findTermStart(entry *LogEntry) (startIndex LogIndex) {
	// TODO: build an map to find these index positions in O(1) time
	startIndex = entry.Index
	for ; startIndex > 0 && l.getEntryAt(startIndex).Term == entry.Term; startIndex-- {
	}
	startIndex = startIndex + 1
	return startIndex
}

func (l *LogImpl) getEntryAt(index LogIndex) (entry *LogEntry) {
	effectiveIndex := index - l.startIndex
	return l.entries[effectiveIndex]
}

func (l *LogImpl) getEntriesFrom(index LogIndex) (prevEntry *LogEntry, entries []*LogEntry) {
	effectiveIndex := index - l.startIndex
	prevIndex := effectiveIndex - 1
	if prevIndex >= 0 {
		prevEntry = l.entries[prevIndex]
	}

	if int(effectiveIndex) < len(l.entries) {
		entries = l.entries[int(effectiveIndex):]
	}

	return
}

func (l *LogImpl) truncateLogAt(index LogIndex) {
	effectiveIndex := index - l.startIndex
	l.entries = l.entries[:effectiveIndex]
}

func (l *LogImpl) nextIndex() LogIndex {
	return l.startIndex + LogIndex(len(l.entries))
}

func (l *LogImpl) insertReplicatedEntries(entries []*LogEntry) {
	for _, entry := range entries {
		nextIndex := l.nextIndex()
		if entry.Index > nextIndex {
			DPrintf(l.rf.me, cmpLogger, "Unexpected replicated entry @%d, expecting %d.", entry.Index, nextIndex)
			panic(fmt.Sprintf("Unexpected replicated entry @%d, expecting %d.", entry.Index, nextIndex))
		}

		// entry index already in log, check if there is a conflict
		if entry.Index < nextIndex {
			existingEntry := l.getEntryAt(entry.Index)
			if existingEntry.Term == entry.Term {
				DPrintf(l.rf.me, cmpLogger, "Already have %d@T%d, skipping.", entry.Index, entry.Term)
			}

			DPrintf(l.rf.me, cmpLogger, "Conflict detected %d@T%d != replicated entry %d@T%d. Truncating log.",
				existingEntry.Index, existingEntry.Term, entry.Index, entry.Term)

			l.truncateLogAt(entry.Index)
		}

		DPrintf(l.rf.me, cmpLogger, "Appending replicated entry %d@T%d", entry.Index, entry.Term)
		l.entries = append(l.entries, entry)
	}

	l.rf.persist()
}

func (l *LogImpl) lastLogEntry() (index LogIndex, term Term) {
	lastEntry := l.getEntryAt(l.nextIndex() - 1)
	return lastEntry.Index, lastEntry.Term
}

func (l *LogImpl) save(encoder *labgob.LabEncoder) error {
	if err := encoder.Encode(int(l.startIndex)); err != nil {
		return fmt.Errorf("failed to encode log start index %v", err)
	}

	numEntries := len(l.entries) - 1
	if err := encoder.Encode(numEntries); err != nil {
		return fmt.Errorf("failed to encode num of log entries %v", err)
	}

	for i, entry := range l.entries {
		if i == 0 {
			continue
		}

		if err := encoder.Encode(entry); err != nil {
			return fmt.Errorf("failed to encode log entry %d: %v", i, err)
		}
	}

	DPrintf(l.rf.me, cmpPersist, "saved %d log entries", numEntries)

	return nil
}

func (l *LogImpl) load(decoder *labgob.LabDecoder) error {
	var savedStartIndex int
	if err := decoder.Decode(&savedStartIndex); err != nil {
		return fmt.Errorf("failed to decode log start index %v", err)
	}
	l.startIndex = LogIndex(savedStartIndex)

	var savedNumEntries int
	if err := decoder.Decode(&savedNumEntries); err != nil {
		return fmt.Errorf("failed to decode number of log entries %v", err)
	}

	for i := 0; i < savedNumEntries; i++ {
		savedEntry := LogEntry{}
		if err := decoder.Decode(&savedEntry); err != nil {
			return fmt.Errorf("failed to decode of log entry %d: %v", i, err)
		}
		l.entries = append(l.entries, &savedEntry)
	}

	DPrintf(l.rf.me, cmpPersist, "loaded %d log entries", savedNumEntries)

	return nil
}
