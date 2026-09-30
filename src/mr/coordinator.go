package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	mu                      sync.Mutex
	Tasks                   []Task
	MapTasks                []Task
	ReduceTasks             []Task
	NReduce                 int
	NMap                    int
	hasMapTasksCompleted    bool
	hasReduceTasksCompleted bool
	Loop                    bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(request RequestTask, task *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.MapTasks) != 0 && c.hasMapTasksCompleted == false {
		for index, mapTask := range c.MapTasks {
			if mapTask.Status == TaskStatusIdle {
				c.MapTasks[index].Status = TaskStatusInProgress
				*task = c.MapTasks[index]
				return nil
			}
		}

		c.hasMapTasksCompleted = isMapTasksCompleted(c.MapTasks)
	}

	if len(c.ReduceTasks) != 0 && c.hasMapTasksCompleted && c.hasReduceTasksCompleted == false {
		for index, reduceTask := range c.ReduceTasks {
			if reduceTask.Status == TaskStatusIdle {
				c.ReduceTasks[index].Status = TaskStatusInProgress
				*task = c.ReduceTasks[index]
				return nil
			}
		}
		c.hasReduceTasksCompleted = isReduceTasksCompleted(c.ReduceTasks)
	}
	if c.hasMapTasksCompleted == true && c.hasReduceTasksCompleted == true {
		task.Loop = false
	}
	return nil
}

func (c *Coordinator) ReportTaskCompletion(request *TaskCompletionRequest, reply *TaskCompletionReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

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
	mapTasks := make([]Task, len(files))
	reduceTasks := make([]Task, nReduce)

	for index, file := range files {
		mapTasks[index] = buildMapTask(file, index, nReduce, TaskTypeMap, len(files))
	}

	for i := 0; i < nReduce; i++ {
		reduceTasks[i] = buildMapTask("", i, nReduce, TaskTypeReduce, len(files))
	}

	c.NReduce = nReduce
	c.NMap = len(files)
	c.MapTasks = mapTasks[:]
	c.ReduceTasks = reduceTasks[:]
	c.Loop = true

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
		Loop:     true,
	}
	return task
}

func isMapTasksCompleted(mapTasks []Task) bool {
	for _, task := range mapTasks {
		if task.Status != TaskStatusCompleted {
			return false
		}
	}
	return true
}

func isReduceTasksCompleted(reduceTasks []Task) bool {
	for _, task := range reduceTasks {
		if task.Status != TaskStatusCompleted {
			return false
		}
	}
	return true
}
