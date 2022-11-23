package raft

import (
	"fmt"
	"log"
	"os"
)

var (
	Logger *log.Logger
)

func init() {
	logFileName := os.Getenv("LOGFILE")
	logFile := os.Stdout
	if logFileName != "" {
		var err error
		logFile, err = os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Fatal(err)
		}
	}
	Logger = log.New(logFile, "", log.Ltime|log.Lmicroseconds)
}

func DPrintf(serverId ServerId, topic string, format string, a ...interface{}) {
	prefix := ""

	prefix = prefix + fmt.Sprintf("[S%d][%v] S%d ", serverId, topic, serverId)
	format = prefix + format
	Logger.Printf(format, a...)
}

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
