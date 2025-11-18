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
	c.createMapTasks()

	c.server() //don't carry on process 不是占有程序（让相应的gorutine结束）的原因，
	go c.checkTimeOut()
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

func (c *Coordinator) AssignTask(args *ExampleArgs, reply *task4Assign) {
	mu.Lock()
	defer mu.Unlock()
	if len(c.taskQueue) > 0 {
		*reply = *<-c.taskQueue //a deep copy （task is a go struct value type（not reference type））
		c.taskMeta[reply.taskID].taskStatus = Running
		c.taskMeta[reply.taskID].startTime = time.Now()
	} else if c.coordinatePhase == FinishPhase {
		*reply = task4Assign{
			taskType: FinishPhase,
		}
	} else {
		*reply = task4Assign{
			taskType: WaitPhase,
		}
	}
}

func (c *Coordinator) TaskCompleted(task *task4Assign, reply *ExampleReply) {
	mu.Lock()
	defer mu.Unlock()
	if task.taskType != c.coordinatePhase || c.taskMeta[task.taskID].taskStatus == Finished {
		return
	}
	c.taskMeta[task.taskID].taskStatus = Finished
	c.processTaskResult(task)
}

func (c *Coordinator) processTaskResult(task *task4Assign) {
	switch task.taskType {
	case MapPhase:
		for reduceTaskID, filePath := range task.intermediates {
			c.intermediates[reduceTaskID] = append(c.intermediates[reduceTaskID], filePath)
		}
		if c.phaseTasksFinished() {
			c.coordinatePhase = ReducePhase
			c.createReduceTasks()
		}
	case ReducePhase:
		//todo: write to output file
		c.coordinatePhase = FinishPhase
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

func (c *Coordinator) checkTimeOut() {
	for _, task := range c.taskMeta {
		mu.Lock()
		time.Sleep(5 * time.Second)
		if task.taskStatus == Running && time.Since(task.startTime) > 10*time.Second {
			task.taskStatus = Idle
			c.taskQueue <- task.taskReference
		}
		mu.Unlock()
	}
}
