package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		reply, ok := CallRequestWork()
		if !ok {
			return
		}

		switch reply.Type {
		case Map:
			log.Printf("Received map task id: %d, filename: %s\n", reply.TaskId, reply.FileName)
			if err := runMapTask(mapf, reply.TaskId, reply.FileName, reply.Buckets); err != nil {
				log.Fatalf("Map task failed: %v", err)
				return
			}
		}

	}
}

func runMapTask(mapf func(string, string) []KeyValue, taskId int, filename string, buckets int) error {
	log.Printf("Running map task id: %d, filename: %s\n", taskId, filename)

	var kva []KeyValue
	var err error
	if kva, err = mapInput(mapf, filename); err != nil {
		return err
	}

	if err = storeIntermediate(kva, taskId, buckets); err != nil {
		return err
	}

	return nil
}

func mapInput(mapf func(string, string) []KeyValue, filename string) ([]KeyValue, error) {
	log.Printf("Mapping input from: %s\n", filename)

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return nil, err
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return nil, err
	}

	return mapf(filename, string(content)), nil
}

func storeIntermediate(results []KeyValue, taskId, buckets int) error {
	files, err := createIntermediateFiles(buckets, taskId)
	if err != nil {
		return err
	}

	for _, file := range files {
		defer file.Close()
	}

	var encoders []*json.Encoder
	for _, file := range files {
		if file == nil {
			panic("no file")
		}

		encoders = append(encoders, json.NewEncoder(file))
	}

	for _, kv := range results {
		bucket := ihash(kv.Key) % buckets
		err := encoders[bucket].Encode(&kv)

		if err != nil {
			file := files[bucket]
			return fmt.Errorf("failed to write map key/value to intermediate output %s: %v",
				file.Name(), err)
		}
	}

	return nil
}

func createIntermediateFiles(buckets int, taskId int) ([]*os.File, error) {
	var files []*os.File

	for i := 0; i < buckets; i++ {
		name := fmt.Sprintf("mr-%d-%d", taskId, i)
		file, err := os.Create(name)

		if err != nil {
			return nil, fmt.Errorf("failed to create intermediate output file: %v", err)
		}

		files = append(files, file)
	}

	return files, nil
}

//
// Call RequestWork RPC to request a task from the coordinator
//
func CallRequestWork() (RequestWorkReply, bool) {

	// declare an argument structure.
	args := RequestWorkArgs{}

	// declare a reply structure.
	reply := RequestWorkReply{}

	ok := call("Coordinator.RequestWork", &args, &reply)
	if ok {
		fmt.Printf("reply = %v\n", reply)
		return reply, true
	} else {
		fmt.Printf("call failed!\n")
		return reply, false
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
