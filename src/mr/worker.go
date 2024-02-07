package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"

	"github.com/google/uuid"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		taskID, task, filename, nReduce, reducer := RequestForTask(workerID)

		if task == NONE {
			break

		} else if task == MAP {
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

			AddToReduceBuckets(kva, taskID, nReduce)

		} else if task == REDUCE {

			file, err := os.Open(fmt.Sprint("mrinterim-", reducer))
			if err != nil {
				log.Fatal(err)
			}

			var intermediate []KeyValue
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}

			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprint("mr-out-", reducer)
			ofile, _ := os.Create(oname)

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

			ofile.Close()
		}

	}

}

func AddToReduceBuckets(kva []KeyValue, mapTaskId uuid.UUID, nReduce int) {
	for _, kv := range kva {
		reduceBucket := ihash(kv.Key) % nReduce

		bucketFileName := fmt.Sprint("mrinterim-", reduceBucket)

		file, ferr := os.OpenFile(bucketFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if ferr != nil {
			log.Fatal(ferr)
		}

		enc := json.NewEncoder(file)
		jerr := enc.Encode(&kv)
		if jerr != nil {
			log.Fatal(jerr)
		}

		file.Close()
	}
}

func RequestForTask(workerID uuid.UUID) (uuid.UUID, TaskType, string, int, int) {
	req := Request{
		WorkerID: workerID,
	}

	res := Response{}

	ok := call("Coordinator.GetTask", &req, &res)
	if ok {
		fmt.Printf("Task Type: %v \n", res.TaskType)
	} else {
		fmt.Printf("call failed!\n")
	}

	return res.TaskID, res.TaskType, res.FileName, res.NReduce, res.Reducer
}

func UpdateTaskStatus(workerID uuid.UUID, taskID uuid.UUID, taskType TaskType, status TaskState, filename string) {
	req := Request{
		WorkerID: workerID,
		TaskID:   taskID,
		TaskType: taskType,
		Status:   status,
		FileName: filename,
	}

	res := Response{}

	ok := call("Coordinator.ReportTask", &req, &res)
	if !ok {
		log.Fatal("Could not report task ", taskType, " : ", taskID, " back to coordinator")
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

func checkFileExists(filePath string) bool {
	_, error := os.Stat(filePath)
	//return !os.IsNotExist(err)
	return !errors.Is(error, os.ErrNotExist)
}
