package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	for {
		reply, ok := CallRequestWork()
		if !ok {
			return
		}

		switch reply.Type {
		case Map:
			log.Printf("Received map task id: %d, inputFiles: %s\n", reply.TaskId, reply.FileNames)
			filenames, err := runMapTask(mapf, reply.TaskId, reply.FileNames[0], reply.Buckets)
			if err != nil {
				log.Fatalf("Map task failed: %v", err)
				return
			}
			CallCompleteTask(Map, reply.TaskId, filenames)

		case Reduce:
			log.Printf("Received reduce task id: %d", reply.TaskId)
			outputFile, err := runReduceTask(reducef, reply.TaskId, reply.FileNames)
			if err != nil {
				log.Fatalf("Reduce task failed: %v", err)
				return
			}
			CallCompleteTask(Reduce, reply.TaskId, []string{outputFile})
		}
	}
}

func runMapTask(mapf func(string, string) []KeyValue, taskId int, filename string, buckets int) ([]string, error) {
	log.Printf("Running map task id: %d, inputFiles: %s\n", taskId, filename)

	var kva []KeyValue
	var err error
	if kva, err = mapInput(mapf, filename); err != nil {
		return nil, err
	}

	var filenames []string
	if filenames, err = storeIntermediate(kva, taskId, buckets); err != nil {
		return nil, err
	}

	return filenames, nil
}

func runReduceTask(reducef func(string, []string) string, taskId int, files []string) (string, error) {
	oname := fmt.Sprintf("mr-out-%d", taskId)
	ofile, err := os.Create(oname)
	if err != nil {
		return "", fmt.Errorf("reduce task: %d, failed to create output file: %v", taskId, err)
	}
	defer ofile.Close()

	intermediate, err := loadIntermediate(files)
	if err != nil {
		return "", fmt.Errorf("reduce task: %d, failed to load intermediate files: %v", taskId, err)
	}

	sort.Sort(ByKey(intermediate))

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	return oname, nil
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

func storeIntermediate(results []KeyValue, taskId, buckets int) ([]string, error) {
	files, err := createIntermediateFiles(buckets, taskId)
	if err != nil {
		return nil, err
	}

	var filenames []string
	for _, file := range files {
		filenames = append(filenames, file.Name())
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
			return nil, fmt.Errorf("failed to write map key/value to intermediate output %s: %v",
				file.Name(), err)
		}
	}

	return filenames, nil
}

func loadIntermediate(filenames []string) ([]KeyValue, error) {
	var results []KeyValue
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			return nil, fmt.Errorf("failed to load intermediate file: %v", err)
		}
		defer file.Close()

		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				return nil, fmt.Errorf("failed to decode intermediate file: %v", err)
			}
			results = append(results, kv)
		}
	}
	return results, nil
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

func CallCompleteTask(taskType TaskType, taskId int, filenames []string) bool {
	args := CompleteTaskArgs{Type: taskType, TaskId: taskId, FileNames: filenames}
	reply := CompleteTaskReply{}

	return call("Coordinator.CompleteTask", &args, &reply)
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
