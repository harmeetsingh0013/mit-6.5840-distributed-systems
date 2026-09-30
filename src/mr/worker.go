package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	for {
		request := &RequestTask{}
		taskReply := &RequestTaskReply{}

		ok := call("Coordinator.AssignTask", &request, &taskReply)
		if !ok {
			fmt.Printf("Failed to get the Task!\n")
		}

		switch taskReply.Status {
		case Wait:
			time.Sleep(100 * time.Millisecond)
			continue
		case Exit:
			return
		case Task_:
			task := taskReply.Task
			switch task.Type {
			case TaskTypeMap:
				if err := mapData(task, mapf); err == nil {
					taskCompletionRequest := &TaskCompletionRequest{
						ID:   task.ID,
						Type: task.Type,
					}
					taskCompletionReply := &TaskCompletionReply{}

					ok = call("Coordinator.ReportTaskCompletion", &taskCompletionRequest, &taskCompletionReply)
					if !ok {
						fmt.Printf("Failed to Report Task Completion!\n")
					}
				}
			case TaskTypeReduce:
				reduceData(task, reducef)
				taskCompletionRequest := &TaskCompletionRequest{
					ID:   task.ID,
					Type: task.Type,
				}
				taskCompletionReply := &TaskCompletionReply{}
				ok = call("Coordinator.ReportTaskCompletion", &taskCompletionRequest, &taskCompletionReply)
				if !ok {
					fmt.Printf("Failed to Report Task Completion!\n")
				}
			default:
				fmt.Printf("No new task exists: %v\n", task.Type)
			}
		default:
			fmt.Printf("Unknown status received: %v\n", taskReply.Status)
		}

	}

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}

func mapData(task Task, mapf func(string, string) []KeyValue) error {
	filename, content := readFile(task.FileName)
	kva := mapf(filename, string(content))

	partions := make(map[int][]KeyValue)

	for _, kv := range kva {
		reduceTaskNumber := ihash(kv.Key) % task.NReduce
		partions[reduceTaskNumber] = append(partions[reduceTaskNumber], kv)
	}

	for reduceTaskNumber, kvs := range partions {
		outputFileName := fmt.Sprintf("mr-%d-%d", task.ID, reduceTaskNumber)
		file, err := os.Create(outputFileName)
		if err != nil {
			log.Fatalf("cannot create %v", outputFileName)
		}
		defer file.Close()
		encoder := json.NewEncoder(file)

		if err := encoder.Encode(kvs); err != nil {
			fmt.Println("Error encoding JSON to file:", err)
			return err
		}
	}

	return nil
}

func reduceData(task Task, reducef func(string, []string) string) error {
	keyValue := make(map[string][]string)
	oname := "mr-out-" + strconv.Itoa(task.ID)
	ofile, _ := os.Create(oname)

	for i := 0; i < task.NMap; i++ {
		inputFileName := fmt.Sprintf("mr-%d-%d", i, task.ID)
		file, err := os.Open(inputFileName)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue // empty partition
			}
			log.Fatalf("cannot read %v", inputFileName)
			return err // real I/O error
		}
		defer file.Close()

		var kva []KeyValue
		decoder := json.NewDecoder(file)
		if err := decoder.Decode(&kva); err != nil {
			fmt.Println("Error decoding JSON from file:", err)
			return err
		}

		for _, kv := range kva {
			keyValue[kv.Key] = append(keyValue[kv.Key], kv.Value)
		}
	}

	for key, val := range keyValue {
		result := reducef(key, val)
		fmt.Fprintf(ofile, "%v %v\n", key, result)
	}
	return nil
}

func readFile(filename string) (string, string) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return filename, string(content)
}
