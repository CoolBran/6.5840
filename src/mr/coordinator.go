package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type taskType int

const (
	Map taskType = iota
	Reduce
	Wait
	Exit
)

type taskStatus int

const (
	Idle taskStatus = iota
	processing
	Completed
)

type Task struct {
	Filename     string
	TaskID       int
	TaskType     taskType
	NReduce      int
	Intermediate []string
}

type task4Assign struct {
	taskStatus taskStatus
	startTime  time.Time
	originTask *Task
}

type Coordinator struct {
	// Your definitions here.
	taskChan     chan *Task
	taskMeta     map[int]*task4Assign
	taskPhase    taskType
	nReduce      int
	filepaths    []string
	intermediate [][]string
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

func (c *Coordinator) AssignTask(arg *ExampleArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	if len(c.taskChan) > 0 {
		*reply = *<-c.taskChan

		c.taskMeta[reply.TaskID].taskStatus = processing
		c.taskMeta[reply.TaskID].startTime = time.Now()
	} else if c.taskPhase == Exit {
		*reply = Task{TaskType: Exit}
	} else {
		*reply = Task{TaskType: Wait}
	}
	return nil
}

func (c *Coordinator) CompleteTask(task *Task, reply *ExampleReply) error {
	mu.Lock()
	defer mu.Unlock()
	if task.TaskType != c.taskPhase || c.taskMeta[task.TaskID].taskStatus == Completed {
		return nil
	}
	c.taskMeta[task.TaskID].taskStatus = Completed
	go c.processTaskResult(task) //go or not[lead to deadlock]
	return nil
}

func (c *Coordinator) processTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	switch task.TaskType {
	case Map:
		//收集intermediate信息
		for reduceTaskId, filePath := range task.Intermediate {
			c.intermediate[reduceTaskId] = append(c.intermediate[reduceTaskId], filePath)
		}
		if c.phaseTaskDone() {
			//获得所以map task后，进入reduce阶段
			c.createReduceTask()
			fmt.Println("Into reduce phase now")
			c.taskPhase = Reduce
		}
	case Reduce:
		if c.phaseTaskDone() {
			//获得所以reduce task后，进入exit阶段
			c.taskPhase = Exit
		}
	}
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
	ret := c.taskPhase == Exit
	return ret
}
func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.taskChan = make(chan *Task, max(len(files), nReduce))
	c.taskMeta = make(map[int]*task4Assign)
	c.taskPhase = Map
	c.filepaths = files
	c.nReduce = nReduce
	c.intermediate = make([][]string, nReduce)

	c.createMapTask()

	c.server()

	go c.catchTimeOut()

	return &c
}

func (c *Coordinator) createMapTask() {
	for idx, filename := range c.filepaths {
		taskMeta := Task{
			Filename: filename,
			TaskType: Map,
			NReduce:  c.nReduce,
			TaskID:   idx,
		}
		c.taskChan <- &taskMeta
		c.taskMeta[idx] = &task4Assign{
			taskStatus: Idle,
			originTask: &taskMeta,
		}
	}
}

func (c *Coordinator) catchTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()
		if c.taskPhase == Exit {
			mu.Unlock()
			return
		}
		for _, task := range c.taskMeta {
			if task.taskStatus == processing && time.Since(task.startTime) > 10*time.Second {
				c.taskChan <- task.originTask
				task.taskStatus = Idle
			}
		}
		mu.Unlock()
	}
}

func (c *Coordinator) createReduceTask() {
	c.taskMeta = make(map[int]*task4Assign)
	for idx, files := range c.intermediate {
		taskMeta := Task{
			TaskType:     Reduce,
			NReduce:      c.nReduce,
			TaskID:       idx,
			Intermediate: files,
		}
		c.taskChan <- &taskMeta
		c.taskMeta[idx] = &task4Assign{
			taskStatus: Idle,
			originTask: &taskMeta,
		}
	}
}

func (c *Coordinator) phaseTaskDone() bool {
	for _, task := range c.taskMeta {
		if task.taskStatus != Completed {
			return false
		}
	}
	return true
}
