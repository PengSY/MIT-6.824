package mapreduce

import(
	"os"
	"time"
	"fmt"
	"encoding/json"
	"sort"
)

type KeyValueList []KeyValue

//create Len,Swap,Less function for KeyValue type
func(kv KeyValueList) Len() int{
	return len(kv)
}

func(kv KeyValueList) Swap(i,j int){
	kv[i],kv[j]=kv[j],kv[i]
}

func(kv KeyValueList) Less(i,j int) bool{
	return kv[i].Key<kv[j].Key
}

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// You will need to write this function.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTaskNumber) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//

	//create a log for doReduce
	logFile, err := os.Create("doReduceLog")
	timestamp := time.Now().Unix()
	tm := time.Unix(timestamp, 0)
	logFile.WriteString(fmt.Sprintln(tm.Format("2006-01-02 03:04:05 PM")))

	//create output file
	var outputFile *os.File
	outputFile, err = os.Create(outFile)
	if err != nil {
		logFile.WriteString(fmt.Sprintln(err))
		return
	}

	//open intermediate file
	interFile := make([]*os.File, nMap)
	for i := 0; i < nMap; i = i + 1 {
		interFile[i], err = os.Open(reduceName(jobName, i, reduceTaskNumber))
		if err != nil {
			logFile.WriteString(fmt.Sprintln(err))
			return
		}
	}

	//read all key-value pair frome intermediate files
	var kvlist KeyValueList
	for _, file := range interFile {
		var kv KeyValue
		decoder := json.NewDecoder(file)
		err = decoder.Decode(&kv)
		for err == nil {
			kvlist = append(kvlist, kv)
			err = decoder.Decode(&kv)
		}
	}

	//sort the intermediate key-value pair by key
	sort.Sort(kvlist)
	//group value for the same key
	kvmap := make(map[string][]string)
	var valueGroup []string
	for i := 0; i < len(kvlist); i = i + 1 {
		valueGroup = append(valueGroup, kvlist[i].Value)
		if i == len(kvlist) - 1 || kvlist[i + 1].Key != kvlist[i].Key {
			kvmap[kvlist[i].Key] = valueGroup
			valueGroup = make([]string, 0)
		}
	}

	//store the result with json
	encoder := json.NewEncoder(outputFile)
	for key, values := range kvmap {
		err = encoder.Encode(KeyValue{key, reduceF(key, values)})
		if err != nil {
			logFile.WriteString(fmt.Sprintln(err))
			return
		}
	}

	//close files
	err = outputFile.Close()
	if err != nil {
		logFile.WriteString(fmt.Sprintln(err))
		return
	}
	for i := 0; i < nMap; i = i + 1 {
		err = interFile[i].Close()
		if err != nil {
			logFile.WriteString(fmt.Sprintln(err))
			return
		}
	}
	logFile.Close()
}
