package mr

import (
	"os"
	"strconv"
)

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
type WorkerRegisterRequest struct {
}

type WorkerRegisterResponse struct {
	WorkerId   int
	MapNReduce int
}

type WorkRequest struct {
	WorkerId int
}

type WorkResponse struct {
	Filename      string
	IsReduce      bool
	MapTaskId     int
	MapTaskIdList []int
	ReduceTaskId  int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
