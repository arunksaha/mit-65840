package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type KeyValueSlice []KeyValue

// for sorting by key.
func (a KeyValueSlice) Len() int           { return len(a) }
func (a KeyValueSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KeyValueSlice) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		// Request a task from the coordinator
		rpcName := "Coordinator.ProcessTaskRequest"
		taskRequest := TaskRequest{}
		taskResponse := TaskResponse{}
		ok := call(rpcName, &taskRequest, &taskResponse)
		if !ok {
			log.Fatalf("Worker: RPC call '%s' failed\n", rpcName)
		}

		if taskResponse.TaskType == MapTask {
			processMapTask(&taskResponse, mapf)
		} else if taskResponse.TaskType == ReduceTask {
			processReduceTask(&taskResponse, reducef)
		} else if taskResponse.TaskType == WaitTask {
			log.Printf("Worker: WaitTask received, sleeping 1s before retry\n")
			time.Sleep(1 * time.Second)
		} else if taskResponse.TaskType == DoneTask {
			log.Printf("Worker: DoneTask received, exiting\n")
			break
		}
	}
}

func processMapTask(taskResponse *TaskResponse, mapf func(string, string) []KeyValue) {
	log.Printf("Worker processMapTask: %s\n", taskResponse.String())

	// Read the input file
	fileName := taskResponse.FileName
	fileContent, err := os.ReadFile(fileName)
	if err != nil {
		log.Fatalf("Worker: cannot read %v", fileName)
	}

	// Apply the map function
	kvlist := mapf(fileName, string(fileContent))

	// Partition the output into nReduce intermediate files
	nReduce := taskResponse.NumReducers
	intFileNames := make([]string, nReduce)
	intFilePointers := make([]*os.File, nReduce)
	const intFileFlags = os.O_CREATE | os.O_WRONLY | os.O_APPEND | os.O_TRUNC
	const intFilePerms = os.FileMode(0644)

	// Create intermediate files
	for i := 0; i < nReduce; i++ {
		intFileNames[i] = fmt.Sprintf("mr-%d-%d", taskResponse.MapId, i)
		intFilePointers[i], err =
			os.OpenFile(intFileNames[i], intFileFlags, intFilePerms)
		if err != nil {
			log.Fatalf("Worker: cannot create intermediate file %s: %v",
				intFileNames[i], err)
		}
		// log.Printf("Worker: created and opened intermediate file %s\n", intFileNames[i])
	}

	// Write key-value pairs to the appropriate intermediate files
	for _, kv := range kvlist {
		reduceTaskNum := ihash(kv.Key) % nReduce
		mesg := fmt.Sprintf("%s %s\n", kv.Key, kv.Value)
		if len(kv.Key) == 0 {
			log.Printf("Worker: WARNING MapTask empty key in map output from file %s to %s\n",
				fileName, intFileNames[reduceTaskNum])
		}
		if _, err := intFilePointers[reduceTaskNum].WriteString(mesg); err != nil {
			log.Fatalf("Worker: intermediate file %s, write '%s' failed: %v",
				intFileNames[reduceTaskNum], mesg, err)
		}
	}

	// Close all intermediate files
	for i := 0; i < nReduce; i++ {
		intFilePointers[i].Close()
	}

	// Report task completion to the coordinator
	sendProcessTaskComplete(MapTask, taskResponse.MapId)
}

func processReduceTask(taskResponse *TaskResponse, reducef func(string, []string) string) {
	log.Printf("Worker processReduceTask: %s\n", taskResponse.String())

	nFiles := taskResponse.NumInputFiles
	reduceIndex := taskResponse.ReduceId
	intFileNames := findIntermediateFiles(nFiles, reduceIndex)
	log.Printf("Worker: ReduceTask found intermediate files: %v\n", intFileNames)

	// Read all intermediate key-value pairs
	// Build Map: key -> list of values
	kvMap := make(map[string][]string)
	for _, fileName := range intFileNames {
		fileContent, err := os.ReadFile(fileName)
		if err != nil {
			log.Fatalf("Worker: cannot read intermediate file %v: %s\n", fileName, err)
		}

		lines := strings.Split(string(fileContent), "\n")
		for _, line := range lines {
			var key, value string
			fmt.Sscanf(line, "%s %s", &key, &value)
			if len(key) == 0 {
				// log.Printf("Worker: WARNING ReduceTask empty key and value (%s) in intermediate file %s line %d (skipped)\n",
				// 	value, fileName, idx+1)
				continue
			}
			kvMap[key] = append(kvMap[key], value)
		}
	}

	// A list to hold the key-value pairs after reduction
	kvlist := make(KeyValueSlice, 0, len(kvMap))

	// Apply the reduce function and write to output file
	for key, values := range kvMap {
		reducedValue := reducef(key, values)
		kvlist = append(kvlist, KeyValue{Key: key, Value: reducedValue})
	}

	// Sort the kvlist by key
	sort.Sort(kvlist)

	// Create temporary output file and write reduced key-value pairs
	tmpFileName := fmt.Sprintf("mr-out-%d-tmp", reduceIndex)
	// tmpFile, err := os.CreateTemp("", outputTmpFileName)
	tmpFile, err := os.Create(tmpFileName)
	if err != nil {
		log.Fatalf("Worker: cannot create output file %v: %s", tmpFileName, err)
	}
	for _, kv := range kvlist {
		mesg := fmt.Sprintf("%v %v\n", kv.Key, kv.Value)
		_, err = tmpFile.WriteString(mesg)
		if err != nil {
			log.Fatalf("Worker: cannot write to output file %v: %s", tmpFileName, err)
		}
	}
	tmpFile.Close()

	// Move temporary file to final output file
	outputFileName := fmt.Sprintf("mr-out-%d", reduceIndex)
	err = os.Rename(tmpFileName, outputFileName)
	if err != nil {
		log.Fatalf("Worker: cannot rename temp output file %v to final output file %v: %s",
			tmpFileName, outputFileName, err)
	}
	log.Printf("Worker: ReduceTask wrote output file %s\n", outputFileName)

	// Report task completion to the coordinator
	sendProcessTaskComplete(ReduceTask, reduceIndex)
}

// Helper function to send TaskComplete RPC
func sendProcessTaskComplete(taskType TaskType, taskId int) {
	const rpcName = "Coordinator.ProcessTaskComplete"
	taskComplete := TaskComplete{
		TaskType: taskType,
		TaskId:   taskId,
	}
	empty := Empty{}
	ok := call(rpcName, &taskComplete, &empty)
	if !ok {
		log.Fatalf("Worker: RPC call '%s' failed for %s\n", rpcName, taskType.String())
	}
	log.Printf("Worker: Sent TaskComplete RPC: %s ok\n", taskComplete.String())
}

// Helper function to find intermediate files for a reduce task
func findIntermediateFiles(nFiles int, reduceIndex int) []string {
	intermediateFiles := []string{}
	for i := 0; i < nFiles; i++ {
		intermediateFileName := fmt.Sprintf("mr-%d-%d", i, reduceIndex)
		intermediateFiles = append(intermediateFiles, intermediateFileName)
	}
	return intermediateFiles
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

	fmt.Printf("Error in RPC call %s, error: %v\n", rpcname, err)
	return false
}
