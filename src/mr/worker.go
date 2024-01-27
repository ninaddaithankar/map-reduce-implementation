package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"

	"github.com/google/uuid"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Get an id for this worker
	workerID := uuid.New()
	fmt.Println("New worker added with ID : ", workerID)

	for {
		time.Sleep(time.Second)
		task, filename, nReduce := RequestForTask(workerID)

		if task == NONE {
			break
		}

		// Perform Map
		if task == MAP {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()

			fmt.Println("Executing MAP for ", filename)
			kva := mapf(filename, string(content))
			fmt.Println("Prepared total ", len(kva), " intermediate keys for ", filename)
		}

		// Perform Reduce
		if task == REDUCE {
			fmt.Print(nReduce)
		}

	}

}

func RequestForTask(workerID uuid.UUID) (TaskType, string, int) {
	req := Request{
		Ask:      "Task",
		WorkerID: workerID,
	}

	res := Response{}

	ok := call("Coordinator.GetTask", &req, &res)
	if ok {
		fmt.Printf("Task Type: %v \n", res.TaskType)
		fmt.Printf("File Name: %v \n", res.FileName)
	} else {
		fmt.Printf("call failed!\n")
	}

	return res.TaskType, res.FileName, res.NReduce
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
