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
	Tasks       []Task
	MapTasks    []Task
	ReduceTasks []Task
	NReduce     int
	NMap        int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(request Request, task *Task) error {
	var mu sync.Mutex

	if len(c.Tasks) == 0 {
		log.Fatalf("No tasks available!")
	}

	fmt.Printf("Assigning task: %+v\n", c.Tasks[0])

	mu.Lock()
	*task = c.Tasks[0]
	task.Status = TaskStatusInProgress
	c.Tasks = c.Tasks[1:]
	switch task.Type {
	case TaskTypeMap:
		c.MapTasks = append(c.MapTasks, *task)
	case TaskTypeReduce:
		c.ReduceTasks = append(c.ReduceTasks, *task)
	}
	mu.Unlock()
	return nil
}

func (c *Coordinator) ReportTaskCompletion(taskID int, taskType TaskType) error {
	var mu sync.Mutex

	mu.Lock()
	if len(c.Tasks) == 0 && len(c.ReduceTasks) == 0 && len(c.MapTasks) != 0 {
		for i := 0; i < c.NReduce; i++ {
			reduceTask := buildMapTask("", i, c.NReduce, TaskTypeReduce, c.NMap)
			c.Tasks = append(c.Tasks, reduceTask)
		}
	}

	switch taskType {
	case TaskTypeMap:
		for index, task := range c.MapTasks {
			if task.ID == taskID {
				c.MapTasks[index].Status = TaskStatusCompleted
				break
			}
		}
	case TaskTypeReduce:
		for index, task := range c.ReduceTasks {
			if task.ID == taskID {
				c.ReduceTasks[index].Status = TaskStatusCompleted
				break
			}
		}
	}
	mu.Unlock()
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
