package raft

import (
	"log"
)

func init() {
	//debugVerbosity = getVerbosity()
	//debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate))
	log.SetFlags(log.Flags() | log.Lmicroseconds)
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.Printf(format, a...)
	return
}
