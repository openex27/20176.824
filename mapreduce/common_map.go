package mapreduce

import (
	"hash/fnv"
	"encoding/json"
	"io/ioutil"
	"os"
)

// doMap manages one map task: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	contents, err :=ioutil.ReadFile(inFile)
	if err != nil {
	    return
	}
	kvList := mapF(inFile,string(contents))
	fileList := make([]*json.Encoder ,0,nReduce)
	for i:= 0;i<nReduce;i++{
	    filename:= reduceName(jobName, mapTaskNumber,i)
	    f,err := os.OpenFile(filename,os.O_CREATE|os.O_WRONLY, 0644)
	    if err != nil {
		return
	    }
	    fileList = append(fileList, json.NewEncoder(f))
	    defer f.Close()
	}
	for _,entry := range kvList{
	    if err :=fileList[ihash(entry.Key)%nReduce].Encode(&entry);err != nil {
		return
	    }
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
