package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		task := getTask()
		fmt.Printf("task: %v, type of task: %d\n", task, task.TaskType)
		if task.TaskType == Reduce {
			fmt.Println("get the reduce task")
		}
		switch task.TaskType {
		case Map:
			mapper(&task, mapf)
		case Reduce:
			reducer(&task, reducef)
		case Wait:
			time.Sleep(5 * time.Second)
		case Exit:
			return
		}
	}

}

func getTask() Task {
	arg := ExampleArgs{}
	task := Task{}
	call("Coordinator.AssignTask", &arg, &task)

	return task
}

func mapper(task *Task, mapf func(string, string) []KeyValue) {
	content, err := os.ReadFile(task.Filename)
	if err != nil {
		log.Fatalf("Error to ReadFile : %s, err:%v \n", task.Filename, err)
	}
	kvas := mapf(task.Filename, string(content))
	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range kvas {
		hashID := ihash(kv.Key) % task.NReduce
		buckets[hashID] = append(buckets[hashID], kv)
	}
	intermediate := make([]string, 0)
	for i := 0; i < task.NReduce; i++ {
		intermediate = append(intermediate, writeKVs2LocalFile(task.TaskID, i, buckets[i]))
	}
	fmt.Println("intermediate: ", intermediate)
	task.Intermediate = intermediate
	completeTask(task)
}

func writeKVs2LocalFile(x int, y int, kvs []KeyValue) string {
	dir, _ := os.Getwd()
	tmpFile, err := os.CreateTemp(dir, "tmp-*.txt")
	if err != nil {
		log.Fatalf("createTemp error in writeKVs2LocalFile: %v\n", err)
	}
	enc := json.NewEncoder(tmpFile)
	for _, kv := range kvs {
		if err := enc.Encode(kv); err != nil {
			log.Fatal("Fail")
		}
	}
	outName := fmt.Sprintf("mr-%d-%d", x, y)
	os.Rename(tmpFile.Name(), outName)
	return filepath.Join(dir, outName)
}

func reducer(task *Task, reducef func(string, []string) string) {
	intermediate := readFiles2KVs(task.Intermediate)

	sort.Sort(ByKey(intermediate))
	dir, _ := os.Getwd()
	tempFile, err := os.CreateTemp(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
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
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", task.TaskID)
	os.Rename(tempFile.Name(), oname)
	completeTask(task)
}

func readFiles2KVs(files []string) []KeyValue {
	kvs := []KeyValue{}

	for _, filepath := range files {
		file, err := os.Open(filepath)
		if err != nil {
			log.Fatalf("Failed to open file: %v, error:%v\n", file, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				fmt.Println("ERROR Decode kv in readFiles2KVs:", err)
				break
			}
			fmt.Println("read the kv from disk:", kv)
			kvs = append(kvs, kv)
		}
		file.Close()
	}

	return kvs
}

func completeTask(task *Task) {
	reply := ExampleReply{}
	fmt.Println("the completeTask is: ", task)
	call("Coordinator.CompleteTask", task, &reply)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
