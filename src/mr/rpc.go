package mr

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

type RequestTaskRepyStatus string

const (
	Wait  RequestTaskRepyStatus = "wait"
	Exit  RequestTaskRepyStatus = "exit"
	Task_ RequestTaskRepyStatus = "task"
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

type RequestTaskReply struct {
	Status RequestTaskRepyStatus
	Task   Task
}

type TaskCompletionRequest struct {
	ID   int
	Type TaskType
}

type TaskCompletionReply struct{}
