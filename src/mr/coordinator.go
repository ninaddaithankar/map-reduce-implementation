package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"

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
	WAIT
)

type TaskInfo struct {
	id        uuid.UUID
	filename  string
	tasktype  TaskType
	taskstate TaskState
	reducer   int
}

type Coordinator struct {
	// Your definitions here.
	FileNames   []string
	NReduce     int
	beginReduce bool

	q          *Queue
	InProgress map[uuid.UUID]TaskInfo

	TaskToWorker map[uuid.UUID]uuid.UUID
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		FileNames:   files,
		NReduce:     nReduce,
		q:           NewQueue(),
		beginReduce: false,

		TaskToWorker: make(map[uuid.UUID]uuid.UUID),
		InProgress:   make(map[uuid.UUID]TaskInfo),
	}

	for _, file := range files {
		taskID := uuid.New()

		c.q.Push(TaskInfo{
			id:        taskID,
			filename:  file,
			tasktype:  MAP,
			taskstate: IDLE,
		})
	}

	c.server()
	return &c
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(request *Request, response *Response) error {

	task, empty := c.q.Pop()

	if !empty {
		response.TaskID = task.id
		response.TaskType = task.tasktype
		response.NReduce = c.NReduce
		response.Reducer = task.reducer

		// map the task to this worker
		c.TaskToWorker[task.id] = request.WorkerID

		c.InProgress[task.id] = task
	} else if !c.beginReduce {
		if len(c.InProgress) == 0 {
			c.beginReduce = true

			c.AddReduceJobsInQueue()

			return c.GetTask(request, response)

		} else {
			response.TaskType = WAIT
			log.Fatal("Taking too long... Need to add handling for timeout here. ")
		}
	} else {
		response.TaskType = NONE
	}

	return nil
}

func (c *Coordinator) ReportTask(request *Request, response *Response) error {

	if request.Status == COMPLETED {
		task := c.InProgress[request.TaskID]
		task.taskstate = request.Status

		delete(c.InProgress, request.TaskID)
		delete(c.TaskToWorker, request.WorkerID)
	}

	return nil
}

func (c *Coordinator) AddReduceJobsInQueue() {
	for reducer := 0; reducer < c.NReduce; reducer++ {
		c.q.Push(TaskInfo{
			id:        uuid.New(),
			tasktype:  REDUCE,
			reducer:   reducer,
			taskstate: IDLE,
		})
	}
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
