package raft

type LogEntry struct {
	index   int
	term    int
	command interface{}
}

type Log interface {
	append(term int, command interface{}) (newEntry, prevEntry *LogEntry)
	hasEntryAt(index int, term int) bool
	getEntriesFrom(index int) []*LogEntry
	nextIndex() int
}

type LogImpl struct {
	entries []*LogEntry // Entries in the current server's log
}

func MakeLog() Log {
	log := &LogImpl{}
	log.entries = make([]*LogEntry, 1024)

	return log
}

func (l *LogImpl) append(term int, command interface{}) (newEntry, prevEntry *LogEntry) {
	newIndex := len(l.entries)
	if newIndex > 0 {
		prevEntry = l.entries[newIndex-1]
	}
	newEntry = &LogEntry{index: newIndex, term: term, command: command}
	l.entries = append(l.entries, newEntry)

	return
}

func (l *LogImpl) hasEntryAt(index int, term int) bool {
	return false
}

func (l *LogImpl) getEntriesFrom(index int) []*LogEntry {
	return nil
}

func (l *LogImpl) nextIndex() int {
	return len(l.entries)
}
