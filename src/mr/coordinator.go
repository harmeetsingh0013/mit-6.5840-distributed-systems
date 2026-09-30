package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	mu          sync.Mutex
	Tasks       []Task
	MapTasks    []Task
	ReduceTasks []Task
	NReduce     int
	NMap        int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(request RequestTask, task *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.Tasks) == 0 {
		log.Fatalf("No tasks available!")
	}

	*task = c.Tasks[0]
	task.Status = TaskStatusInProgress
	c.Tasks = c.Tasks[1:]
	switch task.Type {
	case TaskTypeMap:
		fmt.Println("MapTask START", *task)
		c.MapTasks = append(c.MapTasks, *task)
	case TaskTypeReduce:
		fmt.Println("ReduceTask START", *task)
		c.ReduceTasks = append(c.ReduceTasks, *task)
	}

	return nil
}

func (c *Coordinator) ReportTaskCompletion(request *TaskCompletionRequest, reply *TaskCompletionReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.Tasks) == 0 && len(c.ReduceTasks) == 0 && len(c.MapTasks) != 0 {
		for i := 0; i < c.NReduce; i++ {
			reduceTask := buildMapTask("", i, c.NReduce, TaskTypeReduce, c.NMap)
			c.Tasks = append(c.Tasks, reduceTask)
		}
	}

	switch request.Type {
	case TaskTypeMap:
		for index, task := range c.MapTasks {
			if task.ID == request.ID {
				c.MapTasks[index].Status = TaskStatusCompleted
				break
			}
		}
	case TaskTypeReduce:
		for index, task := range c.ReduceTasks {
			if task.ID == request.ID {
				c.ReduceTasks[index].Status = TaskStatusCompleted
				break
			}
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	c.mu.Lock()
	defer c.mu.Unlock()

	fmt.Println("Tasks length: ", len(c.Tasks))
	fmt.Println("MapTasks length: ", len(c.MapTasks))
	fmt.Println("ReduceTasks length: ", len(c.ReduceTasks))

	if len(c.Tasks) != 0 {
		return ret
	}

	for _, task := range c.MapTasks {
		if task.Status != TaskStatusCompleted {
			return ret
		}
	}

	for _, task := range c.ReduceTasks {
		if task.Status != TaskStatusCompleted {
			return ret
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	tasks := make([]Task, len(files))

	for index, file := range files {
		tasks[index] = buildMapTask(file, index, nReduce, TaskTypeMap, len(files))
	}

	c.NReduce = nReduce
	c.NMap = len(files)
	c.Tasks = tasks[:]

	c.server(sockname)
	return &c
}

func buildMapTask(filename string, index int, nReduce int, taskType TaskType, nMap int) Task {
	task := Task{
		FileName: filename,
		ID:       index,
		Status:   TaskStatusIdle,
		NReduce:  nReduce,
		Type:     taskType,
		NMap:     nMap,
	}
	return task
}
