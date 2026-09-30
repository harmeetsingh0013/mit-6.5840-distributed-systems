package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

type RequestTask struct{}

type TaskStatus string

const (
	TaskStatusIdle       TaskStatus = "idle"
	TaskStatusInProgress TaskStatus = "in-progress"
	TaskStatusCompleted  TaskStatus = "completed"
)

type TaskType string

const (
	TaskTypeMap    TaskType = "map"
	TaskTypeReduce TaskType = "reduce"
)

type Task struct {
	ID       int
	FileName string
	Status   TaskStatus
	NReduce  int
	NMap     int
	Type     TaskType
	Loop     bool
}

type TaskCompletionRequest struct {
	ID   int
	Type TaskType
}

type TaskCompletionReply struct{}
