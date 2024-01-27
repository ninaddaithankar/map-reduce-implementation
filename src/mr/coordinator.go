package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"

	queue "6.5840/mr/datainterfaces"
	"github.com/google/uuid"
)

type TaskState int

const (
	IDLE TaskState = iota
	INPROGRESS
	COMPLETED
)

type TaskType int

const (
	NONE TaskType = iota
	MAP
	REDUCE
)

type TaskInfo struct {
	filename  string
	tasktype  TaskType
	taskstate TaskState
}

type Coordinator struct {
	// Your definitions here.
	FileNames []string
	NReduce   int
	q         queue.Queue

	TaskToWorker map[uuid.UUID]uuid.UUID
	TasksInhand  map[uuid.UUID]TaskInfo
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		FileNames: files,
		NReduce:   nReduce,
		q:         *queue.NewQueue(),

		TaskToWorker: make(map[uuid.UUID]uuid.UUID),
		TasksInhand:  make(map[uuid.UUID]TaskInfo),
	}

	for _, file := range files {
		taskID := uuid.New()

		c.TasksInhand[taskID] = TaskInfo{
			filename:  file,
			tasktype:  MAP,
			taskstate: IDLE,
		}

		c.q.Push(taskID)
	}

	c.server()
	return &c
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(request *Request, response *Response) error {

	nextTaskID, empty := c.q.Pop()

	if !empty {
		nextTask := c.TasksInhand[nextTaskID]

		response.TaskType = nextTask.tasktype
		response.FileName = nextTask.filename
		response.NReduce = c.NReduce

		// map the task to this worker
		c.TaskToWorker[nextTaskID] = request.WorkerID
	} else {
		response.TaskType = NONE
	}

	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.q.IsEmpty()
}

// start a thread that listens for RPCs from worker.go
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
