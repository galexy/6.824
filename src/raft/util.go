package raft

import (
	"fmt"
	"log"
)

var debug = true

func init() {
	//debugVerbosity = getVerbosity()
	//debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate))
	log.SetFlags(log.Flags() | log.Lmicroseconds)
}

func DPrintf(procId int, topic string, format string, a ...interface{}) {
	if debug {
		prefix := ""

		prefix = prefix + fmt.Sprintf("[S%d][%v] S%d ", procId, string(topic), procId)
		format = prefix + format
		log.Printf(format, a...)
	}
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
