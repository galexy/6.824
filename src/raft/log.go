package raft

import (
	"6.824/labgob"
	"6.824/logger"
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
	getEntriesFrom(index LogIndex) (droppedEntry bool, prevEntry *LogEntry, entries []*LogEntry)
	nextIndex() LogIndex
	insertReplicatedEntries(entries []*LogEntry)
	lastLogEntry() (index LogIndex, term Term)
	save(encoder *labgob.LabEncoder) error
	load(encoder *labgob.LabDecoder) error
	compactAt(index LogIndex)
	discard(newStartIndex LogIndex, newStartTerm Term)
	getStartingIndex() LogIndex
}

type LogImpl struct {
	sentinelIndex LogIndex    // What is the latest index in the snapshot. This index represents 0 in slide
	entries       []*LogEntry // Slice holding active log entries, 1-based indexing with 0 holds sentinel
	rf            *Raft       // Link to main raft
}

func MakeLog(rf *Raft) Log {
	log := &LogImpl{rf: rf}
	log.sentinelIndex = LogIndex(0)
	log.entries = make([]*LogEntry, 1, 1024)
	log.entries[0] = &LogEntry{Index: 0, Term: 0, Command: nil}

	return log
}

func (l *LogImpl) Debug(format string, a ...interface{}) {
	prefix := fmt.Sprintf("[S%d][%v] S%d ", l.rf.me, cmpLogger, l.rf.me)
	logger.DPrintf(prefix+format, a...)
}

func (l *LogImpl) getStartingIndex() LogIndex {
	return l.sentinelIndex
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
	if l.sentinelIndex+LogIndex(len(l.entries)) <= index {
		hasEntry = false
		return
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
	for ; startIndex > l.sentinelIndex && l.getEntryAt(startIndex).Term == entry.Term; startIndex-- {
	}
	startIndex = startIndex + 1
	return startIndex
}

func (l *LogImpl) getEntryAt(index LogIndex) (entry *LogEntry) {
	effectiveIndex := index - l.sentinelIndex
	return l.entries[effectiveIndex]
}

func (l *LogImpl) getEntriesFrom(index LogIndex) (droppedEntry bool, prevEntry *LogEntry, entries []*LogEntry) {
	effectiveIndex := index - l.sentinelIndex
	if effectiveIndex < 1 {
		return true, l.entries[0], nil
	}

	prevEntry = l.entries[effectiveIndex-1]

	if int(effectiveIndex) < len(l.entries) {
		entries = l.entries[int(effectiveIndex):]
	}

	return
}

func (l *LogImpl) truncateLogAt(index LogIndex) {
	effectiveIndex := index - l.sentinelIndex
	l.entries = l.entries[:effectiveIndex]
}

func (l *LogImpl) nextIndex() LogIndex {
	return l.sentinelIndex + LogIndex(len(l.entries))
}

func (l *LogImpl) insertReplicatedEntries(entries []*LogEntry) {
	for _, entry := range entries {
		nextIndex := l.nextIndex()
		if entry.Index > nextIndex {
			l.Debug("Unexpected replicated entry @%d, expecting %d.", entry.Index, nextIndex)
			panic(fmt.Sprintf("Unexpected replicated entry @%d, expecting %d.", entry.Index, nextIndex))
		}

		// entry index already in log, check if there is a conflict
		if entry.Index < nextIndex {
			existingEntry := l.getEntryAt(entry.Index)
			if existingEntry.Term == entry.Term {
				l.Debug("Already have %d@T%d, skipping.", entry.Index, entry.Term)
			}

			l.Debug("Conflict detected %d@T%d != replicated entry %d@T%d. Truncating log.",
				existingEntry.Index, existingEntry.Term, entry.Index, entry.Term)

			l.truncateLogAt(entry.Index)
		}

		l.Debug("Appending replicated entry %d@T%d", entry.Index, entry.Term)
		l.entries = append(l.entries, entry)
	}

	l.rf.persist()
}

func (l *LogImpl) lastLogEntry() (index LogIndex, term Term) {
	lastEntry := l.getEntryAt(l.nextIndex() - 1)
	return lastEntry.Index, lastEntry.Term
}

func (l *LogImpl) save(encoder *labgob.LabEncoder) error {
	if err := encoder.Encode(int(l.sentinelIndex)); err != nil {
		return fmt.Errorf("failed to encode log start index %v", err)
	}

	numEntries := len(l.entries)
	if err := encoder.Encode(numEntries); err != nil {
		return fmt.Errorf("failed to encode num of log entries %v", err)
	}

	for i, entry := range l.entries {
		if err := encoder.Encode(entry); err != nil {
			return fmt.Errorf("failed to encode log entry %d: %v", i, err)
		}
	}

	l.Debug("saved %d log entries", numEntries)

	return nil
}

func (l *LogImpl) load(decoder *labgob.LabDecoder) error {
	var savedStartIndex int
	if err := decoder.Decode(&savedStartIndex); err != nil {
		return fmt.Errorf("failed to decode log start index %v", err)
	}
	l.sentinelIndex = LogIndex(savedStartIndex)

	var savedNumEntries int
	if err := decoder.Decode(&savedNumEntries); err != nil {
		return fmt.Errorf("failed to decode number of log entries %v", err)
	}

	// overwrite sentinel entry
	savedEntry := LogEntry{}
	if err := decoder.Decode(&savedEntry); err != nil {
		return fmt.Errorf("failed to decode sentinel log entry: %v", err)
	}
	l.entries[0] = &savedEntry

	for i := 1; i < savedNumEntries; i++ {
		savedEntry := LogEntry{}
		if err := decoder.Decode(&savedEntry); err != nil {
			return fmt.Errorf("failed to decode of log entry %d: %v", i, err)
		}
		//logger.DPrintf(l.rf.me, cmpPersist, "loaded LogEntry(%v)", &savedEntry)
		l.entries = append(l.entries, &savedEntry)
	}

	l.Debug("loaded %d log entries starting at %d", savedNumEntries, l.sentinelIndex)

	return nil
}

func (l *LogImpl) compactAt(newStartIndex LogIndex) {
	l.Debug("Compacting log at %d.", newStartIndex)

	// Note: l.entries[0] is still the sentinel and will hold newStartIndex as last entry
	l.entries = l.entries[newStartIndex-l.sentinelIndex:]
	l.sentinelIndex = newStartIndex
}

func (l *LogImpl) discard(newSentinelIndex LogIndex, newSentinelTerm Term) {
	l.Debug("Discarding log. discard(I=%d, T=%d)", newSentinelIndex, newSentinelTerm)

	l.sentinelIndex = newSentinelIndex
	l.entries = make([]*LogEntry, 1, 1024)
	l.entries[0] = &LogEntry{Index: newSentinelIndex, Term: newSentinelTerm, Command: nil}
}
