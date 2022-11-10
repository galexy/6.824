package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
)
import "strconv"

type TaskType int

const (
	Map TaskType = iota
	Reduce
)

func (t TaskType) String() string {
	switch t {
	case Map:
		return "Map"
	case Reduce:
		return "Reduce"
	default:
		return fmt.Sprintf("%d", int(t))
	}
}

type RequestWorkArgs struct {
}

type RequestWorkReply struct {
	Type      TaskType
	TaskId    int
	FileNames []string
	Buckets   int
}

type CompleteTaskArgs struct {
	Type      TaskType
	TaskId    int
	FileNames []string
}

type CompleteTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
