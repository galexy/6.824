package logger

import (
	"log"
	"os"
)

var lLogger *log.Logger

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
	lLogger = log.New(logFile, "", log.Ltime|log.Lmicroseconds)
}

func DPrintf(format string, a ...interface{}) {
	lLogger.Printf(format, a...)
}
