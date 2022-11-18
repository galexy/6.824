package raft

import (
	"fmt"
	"log"
)

var debug = false
var indent = false

func init() {
	//debugVerbosity = getVerbosity()
	//debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate))
	log.SetFlags(log.Flags() | log.Lmicroseconds)
}

func DPrintf(procId int, topic string, format string, a ...interface{}) (n int, err error) {
	if debug {
		prefix := ""

		if indent {
			for i := 0; i <= procId; i++ {
				prefix = prefix + "\t|"
			}
		}

		prefix = prefix + fmt.Sprintf("- [%v] S%d ", string(topic), procId)
		format = prefix + format
		log.Printf(format, a...)
	}
	return
}
