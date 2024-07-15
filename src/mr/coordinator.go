package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu                 sync.Mutex
	workerid           int
	Finished           bool
	NReduce            int
	IdleWorkers        []int
	LatestMapTaskId    int
	LatestReduceTaskId int

	MapsFinished      bool
	MapTaskCount      int
	MapTaskTodo       []string
	MapTaskProcessing map[string]int
	MapTaskFinished   []string
	FinishedMapTaskId []int

	Cond                 sync.Cond
	ReduceTaskCount      int
	ReduceTaskTodo       []int
	ReduceTaskProcessing map[int]int
	ReduceTaskFinished   []int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	fmt.Printf("Coordinator.Example called with args: %+v , reply will be : %+v \n", args, reply)
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	fmt.Printf("Listening on : %s \n", l.Addr().String())
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	finished := c.Finished
	c.mu.Unlock()
	return finished
}

func (c *Coordinator) RegisterWorker(args *WorkerRegisterRequest, reply *WorkerRegisterResponse) error {
	c.mu.Lock()
	c.workerid++
	*reply = WorkerRegisterResponse{WorkerId: c.workerid, MapNReduce: c.NReduce}
	fmt.Printf("Registered new worker: %v \n", *reply)
	c.IdleWorkers = append(c.IdleWorkers, c.workerid)
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) RequestWork(args *WorkRequest, reply *WorkResponse) error {
	//decide if its a reduce work or
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.RemoveWorkerFromIdle(args, nil)
	if err != nil {
		return err
	}
	//map task
	if len(c.MapTaskTodo) > 0 {
		err := c.assignMapTask(args, reply)
		if err != nil {
			return err
		}
		return nil
	}
	//ensure all map tasks are finished
	for len(c.MapTaskFinished) != c.MapTaskCount {
		c.Cond.Wait()
		if len(c.MapTaskProcessing) < 1 && (len(c.MapTaskFinished) != c.MapTaskCount) {
			err := c.assignMapTask(args, reply)
			if err != nil {
				return err
			}
			return nil
		}
	}
	//Do reduce task
	if len(c.ReduceTaskTodo) > 0 {
		c.assignReduceTask(args, reply)
		return nil
	}
	//Ensure that all reduce tasks are finished
	for len(c.ReduceTaskFinished) != c.ReduceTaskCount {
		c.Cond.Wait()
		if !c.Finished {
			c.assignReduceTask(args, reply)
			return nil
		}
	}

	return errors.New("NOTHING LEFT TO DO")
}

func (c *Coordinator) assignMapTask(args *WorkRequest, reply *WorkResponse) error {

	workTODO := c.MapTaskTodo[len(c.MapTaskTodo)-1]
	c.MapTaskTodo = c.MapTaskTodo[:len(c.MapTaskTodo)-1]
	c.MapTaskProcessing[workTODO] = args.WorkerId
	c.LatestMapTaskId++
	reply.Filename = workTODO
	reply.IsReduce = false
	reply.MapTaskId = c.LatestMapTaskId
	time.AfterFunc(10*time.Second, func() {
		c.reassignWork(workTODO, args.WorkerId, false, 0)
	})
	fmt.Printf("Assigned map task %s to workerID %d \n", workTODO, args.WorkerId)
	return nil
}

func (c *Coordinator) assignReduceTask(args *WorkRequest, reply *WorkResponse) {
	workTODO := c.ReduceTaskTodo[len(c.ReduceTaskTodo)-1]
	c.ReduceTaskTodo = c.ReduceTaskTodo[:len(c.ReduceTaskTodo)-1]
	c.ReduceTaskProcessing[workTODO] = args.WorkerId
	reply.ReduceTaskId = workTODO
	reply.IsReduce = true
	reply.MapTaskIdList = c.FinishedMapTaskId
	time.AfterFunc(10*time.Second, func() {
		c.reassignWork("", args.WorkerId, true, workTODO)
	})
	fmt.Printf("Assigned reduce task %d to workerID %d \n", workTODO, args.WorkerId)
}

func (c *Coordinator) RemoveWorkerFromIdle(args *WorkRequest, _ *WorkRequest) error {
	var found bool
	var worker_index int
	for i, wid := range c.IdleWorkers {
		if args.WorkerId == wid {
			found = true
			worker_index = i
		}
	}
	if !found {
		return errors.New("GIVEN WORKER NOT FOUND IN IDLE WORKERS")
	}
	// remove workerId from idle workers
	c.IdleWorkers = append(c.IdleWorkers[0:worker_index], c.IdleWorkers[worker_index+1:]...)
	fmt.Printf("Removed worker: %d from idle \n", args.WorkerId)
	return nil
}

func (c *Coordinator) InformWorkDone(what *WorkResponse, n *KeyValue) error {
	c.mu.Lock()
	if !what.IsReduce { //map task
		worker, ok := c.MapTaskProcessing[what.Filename]
		if !ok {
			return errors.New("Worker wasnt processing that task something is wrong")
		}
		delete(c.MapTaskProcessing, what.Filename)
		c.IdleWorkers = append(c.IdleWorkers, worker)
		c.MapTaskFinished = append(c.MapTaskFinished, what.Filename)
		c.FinishedMapTaskId = append(c.FinishedMapTaskId, what.MapTaskId)
		fmt.Printf("Map Task id %d (%s)finished by worker %d \n", what.MapTaskId, what.Filename, worker)
		fmt.Printf("Idle workers: %v \n", c.IdleWorkers)
	} else { //reduce task
		worker, ok := c.ReduceTaskProcessing[what.ReduceTaskId]
		if !ok {
			return errors.New("Worker wasnt processing that task something is wrong")
		}
		delete(c.ReduceTaskProcessing, what.ReduceTaskId)
		c.IdleWorkers = append(c.IdleWorkers, worker)
		c.ReduceTaskFinished = append(c.ReduceTaskFinished, what.ReduceTaskId)
		fmt.Printf("Reduce Task id %d finished by worker %d \n", what.ReduceTaskId, worker)
		fmt.Printf("Idle workers: %v \n", c.IdleWorkers)

	}
	c.mu.Unlock()
	c.checkAllMapsAreDone()
	c.checkFinished()
	return nil
}

func (c *Coordinator) checkAllMapsAreDone() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.MapsFinished && len(c.MapTaskFinished) == c.MapTaskCount {
		c.MapsFinished = true
		c.Cond.Broadcast()
	}
}

func (c *Coordinator) checkFinished() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.Finished && len(c.ReduceTaskFinished) == c.ReduceTaskCount {
		c.Finished = true
		c.Cond.Broadcast()
	}
}

func (c *Coordinator) reassignWork(file string, workerID int, isReduce bool, reduceTaskId int) {
	c.mu.Lock()
	if !isReduce {
		_, ok := c.MapTaskProcessing[file]
		if ok {
			fmt.Printf("Worker %d failed to finish %s in time(Map). idle workers are %+v \n", workerID, file, c.IdleWorkers)
			delete(c.MapTaskProcessing, file)
			c.MapTaskTodo = append(c.MapTaskTodo, file)
			c.IdleWorkers = append(c.IdleWorkers, workerID)
			c.Cond.Signal()
		}
	} else { // reassign
		_, ok := c.ReduceTaskProcessing[reduceTaskId]
		if ok {
			fmt.Printf("Worker %d failed to finish %d in time(Reduce). Reassigning to %+v \n", workerID, reduceTaskId, c.IdleWorkers)
			delete(c.ReduceTaskProcessing, reduceTaskId)
			c.ReduceTaskTodo = append(c.ReduceTaskTodo, reduceTaskId)
			c.IdleWorkers = append(c.IdleWorkers, workerID)
			c.Cond.Signal()
		}
	}
	c.mu.Unlock()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{NReduce: nReduce}
	fmt.Printf("Filenames are %v \n", files)
	c.MapTaskTodo = append(c.MapTaskTodo, files...)
	c.MapTaskProcessing = make(map[string]int)
	for i := 0; i < nReduce; i++ {
		c.ReduceTaskTodo = append(c.ReduceTaskTodo, i)
	}
	c.ReduceTaskProcessing = make(map[int]int)
	c.MapTaskCount = len(files)
	c.ReduceTaskCount = nReduce
	c.Cond = sync.Cond{L: &c.mu}
	c.server()
	return &c
}
