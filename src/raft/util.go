package raft

import (
	"fmt"
	"log"
)

var debug = false

func init() {
	//debugVerbosity = getVerbosity()
	//debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate))
	log.SetFlags(log.Flags() | log.Lmicroseconds)
}

func DPrintf(procId int, topic string, format string, a ...interface{}) (n int, err error) {
	if debug {
		prefix := ""
		for i := 0; i < procId; i++ {
			prefix = prefix + "                                "
		}

		prefix = prefix + fmt.Sprintf("[S%d][%v] -", procId, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
	return
}
