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

type TaskType int

const (
	UnknownTask TaskType = iota
	MapTask
	ReduceTask
	WaitTask
	DoneTask
)

func (tt TaskType) String() string {
	switch tt {
	case UnknownTask:
		return "UnknownTask"
	case MapTask:
		return "MapTask"
	case ReduceTask:
		return "ReduceTask"
	case WaitTask:
		return "WaitTask"
	case DoneTask:
		return "DoneTask"
	default:
		return "UnknownTaskType"
	}
}

type TaskState int

const (
	NotStarted TaskState = iota
	InProgress
	Completed
)

func (ts TaskState) String() string {
	switch ts {
	case NotStarted:
		return "NotStarted"
	case InProgress:
		return "InProgress"
	case Completed:
		return "Completed"
	default:
		return "UnknownTaskState"
	}
}

// TaskMeta holds metadata for a task.
type TaskMeta struct {
	taskState TaskState
	triggerAt time.Time
}

func (x TaskMeta) String() string {
	return "TaskMeta{" +
		"taskState: " + x.taskState.String() +
		// Uncomment it if you want to see trigger time in logs
		// ", triggerAt: " + x.triggerAt.String() +
		"}"
}

// TimeOutSeconds defines the timeout duration for tasks.
const TimeOutSeconds = 10

// Coordinator maintains the state of the MapReduce job.
type Coordinator struct {
	// Your definitions here.

	// Input parameters.
	fileNames      []string // List of input files that need to be processed.
	numMapTasks    int      // Number of map tasks (M). Length of fileNames.
	numReduceTasks int      // Number of reduce tasks (R).

	// Bookkeeping.
	mapTaskStates    []TaskMeta // Track completion status of map tasks. Length = M,
	numMapPending    int        // Number of map tasks yet to be completed.
	reduceTaskStates []TaskMeta // Track completion status of reduce tasks. Length = R.
	numReducePending int        // Number of reduce tasks yet to be completed.

	// Concurrency control.
	rwMutex sync.RWMutex // Mutex to protect shared state, this object is accessed by multiple goroutines.
}

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

	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	ret := c.numMapPending == 0 && c.numReducePending == 0

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.fileNames = files
	c.numMapTasks = len(files)
	c.numReduceTasks = nReduce
	c.mapTaskStates = make([]TaskMeta, c.numMapTasks)
	for idx := range c.mapTaskStates {
		c.mapTaskStates[idx].taskState = NotStarted
	}
	c.numMapPending = c.numMapTasks
	c.reduceTaskStates = make([]TaskMeta, c.numReduceTasks)
	for idx := range c.reduceTaskStates {
		c.reduceTaskStates[idx].taskState = NotStarted
	}
	c.numReducePending = c.numReduceTasks

	log.Printf("Created coordinator: %s\n", c.String())

	c.server()
	return &c
}

func (c *Coordinator) ProcessTaskRequest(
	input *TaskRequest, output *TaskResponse) error {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	log.Printf("Coordinator ProcessTaskRequest: received request\n")

	c.pruneTimedOutTasks()

	numMapTasksCompleted := CountOfState(c.mapTaskStates, Completed)
	numReduceTasksCompleted := CountOfState(c.reduceTaskStates, Completed)

	nextMapIndex := IndexOfState(c.mapTaskStates, NotStarted)
	nextReduceIndex := IndexOfState(c.reduceTaskStates, NotStarted)

	assignMapTask := ValidIndex(c.mapTaskStates, nextMapIndex)
	assignReduceTask := (numMapTasksCompleted == c.numMapTasks) &&
		ValidIndex(c.reduceTaskStates, nextReduceIndex)
	assignDoneTask := (numMapTasksCompleted == c.numMapTasks) &&
		(numReduceTasksCompleted == c.numReduceTasks)
	waitingForMap := !ValidIndex(c.mapTaskStates, nextMapIndex) &&
		(numMapTasksCompleted < c.numMapTasks)
	waitingForReduce := !ValidIndex(c.reduceTaskStates, nextReduceIndex) &&
		(numReduceTasksCompleted < c.numReduceTasks)
	assignWaitTask := waitingForMap || waitingForReduce

	if assignMapTask {
		c.mapTaskStates[nextMapIndex].taskState = InProgress
		c.mapTaskStates[nextMapIndex].triggerAt = time.Now()

		output.TaskType = MapTask
		output.MapId = nextMapIndex
		output.FileName = c.fileNames[nextMapIndex]
		output.NumReducers = c.numReduceTasks
	} else if assignWaitTask {
		// All map tasks are in progress. Instruct worker to wait.
		output.TaskType = WaitTask
	} else if assignReduceTask {
		c.reduceTaskStates[nextReduceIndex].taskState = InProgress
		c.reduceTaskStates[nextReduceIndex].triggerAt = time.Now()

		output.TaskType = ReduceTask
		output.ReduceId = nextReduceIndex
		output.NumInputFiles = len(c.fileNames)
	} else if assignDoneTask {
		// All tasks are done. Inform 'Done' to worker so that it can exit.
		output.TaskType = DoneTask
	} else {
		log.Fatalf("Coordinator ProcessTaskRequest: unexpected condition: "+
			"No valid task to assign. Cordinator state: %s\n", c.String())
	}

	log.Printf("Coordinator ProcessTaskRequest: sent %s\n", output.String())
	log.Printf("Coordinator state: %s\n", c.String())

	return nil
}

func (c *Coordinator) ProcessTaskComplete(
	input *TaskComplete, output *Empty) error {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	log.Printf("Coordinator ProcessTaskComplete: %s\n", input.String())

	if input.TaskType == MapTask {
		if c.mapTaskStates[input.TaskId].taskState == Completed {
			log.Printf("Received duplicate completion for map task %d", input.TaskId)
		}
		c.mapTaskStates[input.TaskId].taskState = Completed
		c.numMapPending -= 1
	} else if input.TaskType == ReduceTask {
		if c.reduceTaskStates[input.TaskId].taskState == Completed {
			log.Printf("Received duplicate completion for reduce task %d", input.TaskId)
		}
		c.reduceTaskStates[input.TaskId].taskState = Completed
		c.numReducePending -= 1
	} else {
		log.Printf("Unknown task type %v in TaskComplete RPC", input.TaskType)
	}

	log.Printf("Coordinator state: %s\n", c.String())
	return nil
}

// pruneTimedOutTasks resets tasks that have been InProgress for too long.
func (c *Coordinator) pruneTimedOutTasks() {
	now := time.Now()
	timeoutDuration := TimeOutSeconds * time.Second

	// Prune map tasks
	for idx, taskMeta := range c.mapTaskStates {
		if taskMeta.taskState == InProgress &&
			now.Sub(taskMeta.triggerAt) > timeoutDuration {
			log.Printf("Coordinator: Map task %d timed out, resetting to NotStarted\n", idx)
			c.mapTaskStates[idx].taskState = NotStarted
		}
	}

	// Prune reduce tasks
	for idx, taskMeta := range c.reduceTaskStates {
		if taskMeta.taskState == InProgress &&
			now.Sub(taskMeta.triggerAt) > timeoutDuration {
			log.Printf("Coordinator: Reduce task %d timed out, resetting to NotStarted\n", idx)
			c.reduceTaskStates[idx].taskState = NotStarted
		}
	}
}

func (c Coordinator) String() string {
	return "Coordinator{" +
		"fileNames: " + fmt.Sprintf("%v", c.fileNames) +
		", NMap: " + fmt.Sprintf("%d", c.numMapTasks) +
		", NReduce: " + fmt.Sprintf("%d", c.numReduceTasks) +
		", mapTaskStates: " + fmt.Sprintf("%v", c.mapTaskStates) +
		", MapPending: " + fmt.Sprintf("%d", c.numMapPending) +
		", reduceTaskStates: " + fmt.Sprintf("%v", c.reduceTaskStates) +
		", ReducePending: " + fmt.Sprintf("%d", c.numReducePending) +
		"}"
}

// IndexOf returns the index of val in slice, or -1 if not found.
func IndexOfState(slice []TaskMeta, target TaskState) int {
	for i, v := range slice {
		if v.taskState == target {
			return i
		}
	}
	return -1
}

// CountOf returns the count of val in slice.
func CountOfState(slice []TaskMeta, target TaskState) int {
	count := 0
	for _, v := range slice {
		if v.taskState == target {
			count++
		}
	}
	return count
}

// ValidIndex checks if index is a valid index for slice.
func ValidIndex[T comparable](slice []T, index int) bool {
	return 0 <= index && index < len(slice)
}

func assert(cond bool, msg string) {
	if !cond {
		panic("assertion failed: " + msg)
	}
}
