package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

type TaskRequest struct {
}

type TaskResponse struct {
	TaskType      TaskType // "map" or "reduce"
	MapId         int      // Map task identifier, used for "map" tasks
	FileName      string   // Input file name for "map" tasks
	NumReducers   int      // Number of target partitions or reducers
	ReduceId      int      // Reduce task identifier, used for "reduce" tasks
	NumInputFiles int      // Number of input files for "reduce" tasks
}

type TaskComplete struct {
	TaskType TaskType // "map" or "reduce"
	TaskId   int      // Identifier of the completed task
}

type Empty struct {
}

func (tr TaskResponse) String() string {
	return "TaskResponse{TaskType: " + tr.TaskType.String() +
		", MapId: " + strconv.Itoa(tr.MapId) +
		", FileName: " + tr.FileName +
		", NumReducers: " + strconv.Itoa(tr.NumReducers) +
		", ReduceId: " + strconv.Itoa(tr.ReduceId) +
		", NumInputFiles: " + strconv.Itoa(tr.NumInputFiles) +
		"}"
}

func (tc TaskComplete) String() string {
	return "TaskComplete{TaskType: " + tc.TaskType.String() +
		", TaskId: " + strconv.Itoa(tc.TaskId) +
		"}"
}
