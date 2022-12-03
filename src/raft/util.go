package raft

func max(a, b LogIndex) LogIndex {
	if a > b {
		return a
	} else {
		return b
	}
}

func min(a, b LogIndex) LogIndex {
	if a < b {
		return a
	} else {
		return b
	}
}
