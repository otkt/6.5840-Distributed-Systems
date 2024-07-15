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
	"sort"
	"strconv"
	"time"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var WorkerInfo WorkerRegisterResponse

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	// Call the coordinator ask for work,

	// if its a map work
	// Call mapf on given file addr, append to key value file, or write? once file is completely read
	// Call coordinator let it know you are done with mapping
	//else if its a reduce work

	// uncomment to send the Example RPC to the coordinator.

	var register_err error = nil
	for {
		register_err = RegisterSelf()
		if register_err == nil {
			break
		} else {
			time.Sleep(time.Second)
		}
	}
	for {
		work, err := requestNewWork()
		if err != nil {
			fmt.Println(err)
			fmt.Println("Exiting")
			return
		}

		fmt.Printf("Processing %v \n", work)
		if !work.IsReduce { // process map
			processMap(work, mapf)
		} else {
			processReduce(work, reducef)
		}
		//TODO: Debug line
		//printCurrentDirectoryContents()
		fmt.Printf("Worker done with task\n")
	}

}
func processReduce(work WorkResponse, reducef func(string, []string) string) {
	var filenames []string
	for _, mapid := range work.MapTaskIdList {
		filename := "mr-" + strconv.Itoa(mapid) + "-" + strconv.Itoa(work.ReduceTaskId)
		filenames = append(filenames, filename)
	}
	fmt.Printf("Processing files %v \n", filenames)

	var intermediate []KeyValue
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("cannot open %v \n", filename)
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	// input vectors to func , write output to work.ReduceTaskId
	ofilename := "mr-out-" + strconv.Itoa(work.ReduceTaskId)
	ofile, _ := os.Create(ofilename)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		val_output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, val_output)
		i = j
	}

	ofile.Close()

	call("Coordinator.InformWorkDone", &work, &KeyValue{})
}
func processMap(work WorkResponse, mapf func(string, string) []KeyValue) {
	filename := work.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		panic(err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	reduceBucket := make(map[int][]KeyValue)
	kva := mapf(filename, string(content))
	for _, res := range kva {
		reduceTaskN := ihash(res.Key) % WorkerInfo.MapNReduce
		_, ok := reduceBucket[reduceTaskN]
		if !ok {
			reduceBucket[reduceTaskN] = []KeyValue{res}
		} else {
			reduceBucket[reduceTaskN] = append(reduceBucket[reduceTaskN], res)
		}
	}
	for filesuffix, res := range reduceBucket {
		fileName := "mr-" + strconv.Itoa(work.MapTaskId) + "-" + strconv.Itoa(filesuffix)
		sort.Sort(ByKey(res))
		ofile, _ := os.Create(fileName)
		enc := json.NewEncoder(ofile)
		for _, keyval := range res {
			enc.Encode(&keyval)
		}
		ofile.Close()
	}
	call("Coordinator.InformWorkDone", &work, &KeyValue{})
}
func RegisterSelf() error {
	args := WorkerRegisterRequest{}

	ok := call("Coordinator.RegisterWorker", &args, &WorkerInfo)
	if ok {
		fmt.Printf("Registered as worker : %v \n", WorkerInfo.WorkerId)
		return nil
	} else {
		fmt.Printf("Worker registeration failed")
		return errors.New("FAILED WORKER REGISTER")
	}
}

func requestNewWork() (WorkResponse, error) {
	worktoDO := WorkResponse{}
	ok := call("Coordinator.RequestWork", &WorkRequest{WorkerId: WorkerInfo.WorkerId}, &worktoDO)
	fmt.Printf("Worktodo %+v \n ", worktoDO)
	if !ok {
		return worktoDO, errors.New("COULDNT GET WORK")
	}
	return worktoDO, nil
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

	fmt.Println(err)
	return false
}

func printCurrentDirectoryContents() error {
	// Get the current working directory
	dir, err := os.Getwd()
	if err != nil {
		return err
	}
	fmt.Println("Current Directory:", dir)

	// List files in the current directory
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}

	fmt.Println("Directory contents:")
	for _, file := range files {
		fmt.Println(file.Name())
	}
	return nil
}
