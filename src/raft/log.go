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
	entries []interface{} // Entries in the current server's log
}

func (l *LogImpl) append(term int, command interface{}) (newEntry, prevEntry *LogEntry) {
	return nil, nil
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
