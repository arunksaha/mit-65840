# Map Reduce High Level Design

In this project, the number of map tasks M is the number of input files, and
the number of reduce tasks R is chosen arbitrarily by the coordinator.
Neither M nor R is necessarily equal to the number of workers.

Lets say, we have M=5, R=4, and W=3

After the map phase, the following intermediate files are expected.

    mr-0-0  // 1st file 1st part
    mr-0-1
    mr-0-2
    mr-0-3  // 1st file last part
    mr-1-0  // 2nd file 1st part
    mr-1-1
    mr-1-2
    mr-1-3  // 2nd file last part
    mr-2-0  // 3rd file 1st part
    mr-2-1
    mr-2-2
    mr-2-3  // 3rd file last part
    mr-3-0  // 4th file 1st part
    mr-3-1
    mr-3-2
    mr-3-3  // 4th file last part
    mr-4-0  // 5th file 1st part
    mr-4-1
    mr-4-2
    mr-4-3  // 5th file last part

Who creates them? The three workers together. 
One scenario among many possibilities is the following.

    W0 mr-0-0
    W0 mr-0-1
    W0 mr-0-2
    W0 mr-0-3
    W1 mr-1-0
    W1 mr-1-1
    W1 mr-1-2
    W1 mr-1-3
    W2 mr-2-0
    W2 mr-2-1
    W2 mr-2-2
    W2 mr-2-3
    W0 mr-3-0
    W0 mr-3-1
    W0 mr-3-2
    W0 mr-3-3
    W1 mr-4-0
    W1 mr-4-1
    W1 mr-4-2
    W1 mr-4-3

Note that, the above assignment is illustrative. In reality, task assignment
is dynamic, driven by worker availability and RPC timing; a requesting worker
is assigned the next pending task.

Simply put, each worker receives a file (name), runs mapping on it, and
saves the result in R result files.
For each emitted key-value pair from `mapf()`, the worker computes
 `reduceIndex = hash(key) % R`
and appends the pair to the file mr-X-reduceIndex, where X is the map task id.

The coordinator is started first. After that, a number of workers are started.
All workers open a socket connection to the coordinator and perform the
following communication.

The communication between the worker and the coordinator progress like this.

    // Worker sends a task request to the coordinator.
    Wi -> C:    TaskRequest {}
    
    // Coordinator sends a response with the task specifics.
    C  -> Wi:   TaskResponse {TaskType=Map MapId=X FileName=pg-foo.txt NumReducers=R}
    
        As part of processing this, the worker creates the following NumReducers files.
            mr-X-0
            mr-X-1
            mr-X-2
            mr-X-3

    // Worker notifies that the task is complete.
    Wi -> C:    TaskComplete {TaskType=Map MapId=2}

Subsequently, the worker requests for the next task. If there are more Map
tasks, the coordinator assigns them in the above way. Once all the map tasks
are completed, the coordinator starts assigning reduce tasks.

After the reduce phase, the following (final) files are expected.

    mr-out-0
    mr-out-1
    mr-out-2
    mr-out-3

The communication between the worker and the coordinator progress like this.

    C  -> Wi:   TaskResponse {TaskType=Reduce NumInputFiles=M ReduceId=Y}

        Process M files of the form mr-*-Y

            Each reducer reads the Y'th partition from every map worker,
            merges all values for each key,
            applies `reducef()` on the merged values,
            sorts the resulting key-value pairs by key, and
            writes to the final output file.

        Create mr-out-Y

    // Worker notifies that the task is complete.
    Wi -> C:    TaskComplete {TaskType=Reduce ReduceId=t67}

Every task can be in one of the following three states:
  - NotStarted: the task is not started
  - InProgress: the task is assigned to some worker
  - Completed: the task is reported completed by some worker
The coordinator tracks these states via TaskState.

It is possible that a worker crashes while processing.
To address that, the coordinator tracks when a task is assigned.
If a task stays in the assigned state (InProgress) for too long (10 seconds),
then coordinator times out, and assigns the same task to another worker.

The TaskResponse sent by the coordinator to a worker may contain
any one of the following four tasks:
  - MapTask: assign a Map task to the worker
  - Reducetask: assign a Reduce task to the worker
  - WaitTask: ask the worker to wait.
  - DoneTask: notifies the worker that the Map-Reduce job is done. The worker may now exit.
  
The WaitTask is useful in a few different scenarios:
  - All map tasks are started and some tasks are still in progress.
    The reduce tasks cannot be started until all map tasks are complete,
    since the intermediate MR files from the incomplete map tasks are unavailable.
  - If some task, either Map or Reduce, times out.
    At that point, the system needs some other worker to reassign the timed out task.
Upon receiving a WaitTask, the worker waits for a while (e.g., one second sleep)
and again queries (TaskRequest) the coordinator.

The coordinator maintains a data structure to track the overall state of the Map-Reduce job.
```
	// Input parameters.
	fileNames      []string // List of input files that need to be processed.
	numMapTasks    int      // Number of map tasks (M). Length of fileNames.
	numReduceTasks int      // Number of reduce tasks (R).

	// Bookkeeping.
	mapTaskStates    []TaskMeta // Track completion status of map tasks. Length = M,
	numMapPending    int        // Number of map tasks yet to be completed.
	reduceTaskStates []TaskMeta // Track completion status of reduce tasks. Length = R.
	numReducePending int        // Number of reduce tasks yet to be completed.
```
