package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type coordinatePhase int

const (
	MapPhase coordinatePhase = iota
	ReducePhase
	WaitPhase   //todo: task assign over but not finished (map or reduce phase)
	FinishPhase //todo: all task finished
)

type taskStatus int

const (
	Idle taskStatus = iota
	Running
	Finished
)

type coordinateTask struct {
	taskStatus    taskStatus
	startTime     time.Time
	taskReference *task4Assign
}

type task4Assign struct {
	//both
	taskType coordinatePhase
	taskID   int //in map task intermediate outname: mr-taskID-reduceIdx  in reduce task outname: mr-out-taskID

	//map
	mapTaskInput string
	reduceNum    int

	//between map and reduce
	intermediates []string

	//reduce
	reduceTaskOutput string
}

type Coordinator struct {
	// Your definitions here.
	coordinatePhase coordinatePhase

	taskQueue chan *task4Assign
	taskMeta  map[int]*coordinateTask

	nReduce       int
	inputFiles    []string
	intermediates [][]string
}

var mu sync.Mutex

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	mu.Lock()
	defer mu.Unlock()
	ret := c.coordinatePhase == FinishPhase
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.coordinatePhase = MapPhase
	c.nReduce = nReduce
	c.inputFiles = files
	c.taskQueue = make(chan *task4Assign, max(len(files), nReduce))
	c.taskMeta = make(map[int]*coordinateTask)
	c.intermediates = make([][]string, nReduce)

	c.server() //don't carry on process 不是占有程序（让相应的gorutine结束）的原因，
	return &c
}

func (c *Coordinator) createMapTasks() {
	for idx, file := range c.inputFiles {
		task := task4Assign{
			taskType:     MapPhase,
			taskID:       idx,
			mapTaskInput: file,
			reduceNum:    c.nReduce,
		}
		c.taskQueue <- &task
		c.taskMeta[idx] = &coordinateTask{
			taskStatus:    Idle,
			taskReference: &task,
		}
	}
}

func (c *Coordinator) createReduceTasks() {
	c.taskMeta = make(map[int]*coordinateTask)
	for idx := 0; idx < c.nReduce; idx++ {
		task := task4Assign{
			taskType:      ReducePhase,
			taskID:        idx,
			intermediates: c.intermediates[idx],
		}
		c.taskQueue <- &task
		c.taskMeta[idx] = &coordinateTask{
			taskStatus:    Idle,
			taskReference: &task,
		}
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (c *Coordinator) phaseTasksFinished() bool {
	for _, task := range c.taskMeta {
		if task.taskStatus != Finished {
			return false
		}
	}
	return true
}
