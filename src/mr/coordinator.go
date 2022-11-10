package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Status int

const (
	Idle Status = iota
	Running
	Complete
)

type MapTask struct {
	id       int
	filename string
	status   Status
	started  time.Time
}

type Coordinator struct {
	buckets    int
	mapTasks   []MapTask
	mapTaskCh  chan MapTask
	completeCh chan CompleteTaskArgs
	doneCh     chan bool
}

// Your code here -- RPC handlers for the worker to call.

//
// Request Work RPC handler.
//
func (c *Coordinator) RequestWork(args *RequestWorkArgs, reply *RequestWorkReply) error {
	log.Println("RPC RequestWork Received")
	log.Println("Checking if any tasks are available")

	mapTask := <-c.mapTaskCh
	log.Printf("Map Task: %d found, filename: %s\n", mapTask.id, mapTask.filename)

	reply.Type = Map
	reply.TaskId = mapTask.id
	reply.FileName = mapTask.filename
	reply.Buckets = c.buckets

	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	log.Println("RPC CompleteTask received for Task Type: %d, Id: %d", args.Type, args.TaskId)
	c.completeCh <- *args

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	log.Println("Checking if coordinator is done")
	select {
	case done := <-c.doneCh:
		log.Println("Yep... done")
		return done
	default:
		log.Println("Nope.. check again")
		return false
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	numMapTasks := len(files)

	c := Coordinator{
		buckets:    nReduce,
		mapTaskCh:  make(chan MapTask, numMapTasks),
		completeCh: make(chan CompleteTaskArgs),
		doneCh:     make(chan bool),
	}

	go func() {
		for i, file := range files {
			mapTask := MapTask{id: i, filename: file}
			c.mapTasks = append(c.mapTasks, mapTask)

			log.Printf("Adding map task: %d, input file: %s\n", mapTask.id, mapTask.filename)
			c.mapTaskCh <- mapTask
		}

		var n = numMapTasks
		for n > 0 {
			select {
			case completeArgs := <-c.completeCh:
				log.Printf("Received complete for task %d\n", completeArgs.TaskId)
				n--
			}
		}

		log.Printf("All %d map tasks have completed.\n", numMapTasks)
		log.Println("Sending signal to done channel")
		c.doneCh <- true
		log.Printf("Done signal consumed")
	}()

	c.server()
	return &c
}
