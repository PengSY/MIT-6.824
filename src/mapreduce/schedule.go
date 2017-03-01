package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	//set DoTaskArgs
	var args DoTaskArgs
	args.JobName = jobName
	args.NumOtherPhase = n_other
	args.Phase = phase

	//undo tasks and this version don't have a doing task list
	undoList := make([]int, ntasks)
	for i := 0; i < ntasks; i = i + 1 {
		undoList[i] = i
	}
	//workers and tasks it is doing
	wtmap := make(map[string]int)
	//workers return its rpc address through channel if success
	rschan := make(chan string)
	//idle worker list
	idleWorker := make([]string, 0)
	//signal there is idleWorker through channel
	//worker num
	totalWorker := 0
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		for {
			select {
			//if there is a worker ask to register
			case workerAddr := <-registerChan:
				idleWorker = append(idleWorker, workerAddr)
				totalWorker = totalWorker + 1
				fmt.Println("registerChan")
			//if a worker finished its task
			case workerAddr := <-rschan:
				idleWorker = append(idleWorker, workerAddr)
				delete(wtmap, workerAddr)
				fmt.Println("rschan")
			default:
				if len(undoList) == 0 {
					if len(idleWorker) == totalWorker {
						fmt.Println("all worker idle")
						defer wg.Done()
						return
					}
				} else if len(idleWorker) > 0 {
					for len(idleWorker) > 0 {
						if len(undoList) == 0 {
							break
						}
						args.TaskNumber = undoList[0]
						undoList = undoList[1:]
						workerAddr := idleWorker[0]
						idleWorker = idleWorker[1:]
						wtmap[workerAddr] = args.TaskNumber
						if phase == mapPhase {
							args.File = mapFiles[args.TaskNumber]
						}
						go func(srv string, rpcname string, args interface{}, reply interface{}) {
							call(srv, rpcname, args, reply)
							fmt.Println("after call")
							rschan <- srv
						}(workerAddr, "Worker.DoTask", args, nil)
					}
				}
			}
		}

	}()

	wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
