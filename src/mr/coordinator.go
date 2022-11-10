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

type Task struct {
	taskType    TaskType
	id          int
	inputFiles  []string
	status      Status
	outputFiles []string
}

type TaskTimeout struct {
	TaskType TaskType
	id       int
}

type Coordinator struct {
	buckets     int
	mapTasks    []*Task
	reduceTasks []*Task
	taskCh      chan Task
	completeCh  chan CompleteTaskArgs
	timeoutCh   chan TaskTimeout
	doneCh      chan bool
}

// Your code here -- RPC handlers for the worker to call.

//
// Request Work RPC handler.
//
func (c *Coordinator) RequestWork(args *RequestWorkArgs, reply *RequestWorkReply) error {
	log.Println("RPC RequestWork Received")
	log.Println("Checking if any tasks are available")

	task := <-c.taskCh
	log.Printf("Task: %v, Id: %d found, inputFiles: %s\n", task.taskType, task.id, task.inputFiles)

	reply.Type = task.taskType
	reply.TaskId = task.id
	reply.FileNames = task.inputFiles
	reply.Buckets = c.buckets

	go func() {
		log.Printf("Setting 10 second timeout for map task: %d", task.id)
		time.Sleep(time.Second * 10)
		c.timeoutCh <- TaskTimeout{TaskType: task.taskType, id: task.id}
	}()

	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	log.Println("RPC CompleteTask received for Task Type: %v, Id: %d", args.Type, args.TaskId)
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
	select {
	case done := <-c.doneCh:
		return done
	default:
		return false
	}
}

func (c *Coordinator) run(files []string, numMapTasks, numReduceTasks int) {
	for i, file := range files {
		mapTask := Task{taskType: Map, id: i, inputFiles: []string{file}}
		c.mapTasks = append(c.mapTasks, &mapTask)

		log.Printf("Adding map task: %d, input file: %s\n", mapTask.id, mapTask.inputFiles)
		c.taskCh <- mapTask
	}

	c.drainTasks(numMapTasks)
	log.Printf("All %d map tasks have completed.\n", numMapTasks)

	reduceInputs := make(map[int][]string)
	for _, mapTask := range c.mapTasks {
		for i, file := range mapTask.outputFiles {
			reduceInputs[i] = append(reduceInputs[i], file)
		}
	}

	for i, files := range reduceInputs {
		reduceTask := Task{taskType: Reduce, id: i, inputFiles: files}
		c.reduceTasks = append(c.reduceTasks, &reduceTask)

		log.Printf("Adding reduce task: %d", i)
		c.taskCh <- reduceTask
	}

	c.drainTasks(numReduceTasks)
	log.Printf("Add %d reduce tasks have completed.\n", numReduceTasks)

	log.Println("Sending signal to done channel")
	c.doneCh <- true
	log.Printf("Done signal consumed")
}

func (c *Coordinator) drainTasks(numTasks int) {
	for numTasks > 0 {
		select {
		case completeArgs := <-c.completeCh:
			log.Printf("Received complete for type: %v, id %d\n", completeArgs.Type, completeArgs.TaskId)
			task := c.getTask(completeArgs.Type, completeArgs.TaskId)

			if task.status == Complete {
				log.Printf("Map Task: %d is already complete. Ignoring duplicate complete.\n", task.id)
			}

			log.Printf("Marking Map Task: %d complete.\n", task.id)
			task.status = Complete
			task.outputFiles = completeArgs.FileNames
			numTasks--

		case taskTimeout := <-c.timeoutCh:
			log.Printf("Task timeout: type: %d, id: %d\n", taskTimeout.TaskType, taskTimeout.id)
			task := c.getTask(taskTimeout.TaskType, taskTimeout.id)
			if task.status == Complete {
				log.Printf("Map Task: %d is already complete. Ignoring timeout.\n", task.id)
				break
			}
			log.Printf("Queuing map task: %d again.\n", task.id)
			c.taskCh <- *task
		}
	}
}

func (c *Coordinator) getTask(taskType TaskType, id int) *Task {
	switch taskType {
	case Map:
		return c.mapTasks[id]
	case Reduce:
		return c.reduceTasks[id]
	default:
		panic("Unexpected task type")
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
		taskCh:     make(chan Task, max(numMapTasks, nReduce)),
		completeCh: make(chan CompleteTaskArgs),
		timeoutCh:  make(chan TaskTimeout),
		doneCh:     make(chan bool),
	}

	go c.run(files, numMapTasks, nReduce)

	c.server()
	return &c
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
