package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
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
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(request RequestTask, task *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.hasMapTasksCompleted && c.hasReduceTasksCompleted {
		task.Status = Exit
		return nil
	}

	if len(c.MapTasks) != 0 && c.hasMapTasksCompleted == false {
		if assignIdleTask(c.MapTasks, task) {
			return nil
		}

		c.hasMapTasksCompleted = allTasksCompleted(c.MapTasks)
	}

	if c.hasMapTasksCompleted == false {
		requeueExpiredTasks(c.MapTasks)
		task.Status = Wait
		return nil
	}

	if len(c.ReduceTasks) != 0 && c.hasMapTasksCompleted && c.hasReduceTasksCompleted == false {
		if assignIdleTask(c.ReduceTasks, task) {
			return nil
		}
		c.hasReduceTasksCompleted = allTasksCompleted(c.ReduceTasks)
	}

	if c.hasReduceTasksCompleted == false {
		requeueExpiredTasks(c.ReduceTasks)
		task.Status = Wait
		return nil
	}
	task.Status = Exit
	return nil
}

func (c *Coordinator) ReportTaskCompletion(request *TaskCompletionRequest, reply *TaskCompletionReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch request.Type {
	case TaskTypeMap:
		markTaskCompleted(c.MapTasks, request.ID)
	case TaskTypeReduce:
		markTaskCompleted(c.ReduceTasks, request.ID)
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

	if !allTasksCompleted(c.MapTasks) {
		return ret
	}
	if !allTasksCompleted(c.ReduceTasks) {
		return ret
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

// assignIdleTask hands the first idle task to the worker. Returns true if one was assigned.
func assignIdleTask(tasks []Task, reply *RequestTaskReply) bool {
	for index, t := range tasks {
		if t.Status == TaskStatusIdle {
			tasks[index].TTL = time.Now().Add(5 * time.Second).UnixMilli()
			tasks[index].Status = TaskStatusInProgress
			reply.Task = tasks[index]
			reply.Status = Task_
			return true
		}
	}
	return false
}

// requeueExpiredTasks puts timed-out in-progress tasks back to idle.
func requeueExpiredTasks(tasks []Task) {
	for index, t := range tasks {
		if t.Status == TaskStatusInProgress {
			if time.Now().UnixMilli() > t.TTL {
				tasks[index].Status = TaskStatusIdle
			}
		}
	}
}

// markTaskCompleted marks the task with the given ID as completed.
func markTaskCompleted(tasks []Task, id int) {
	for index, t := range tasks {
		if t.ID == id {
			tasks[index].Status = TaskStatusCompleted
			break
		}
	}
}

// allTasksCompleted reports whether every task is completed.
func allTasksCompleted(tasks []Task) bool {
	for _, t := range tasks {
		if t.Status != TaskStatusCompleted {
			return false
		}
	}
	return true
}
