package mapreduce

import (
	"os"
	"sort"
	"encoding/json"
)

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
	kvList := KvList{}
	for m := 0; m < nMap; m++ {
		file, err := os.Open(reduceName(jobName, m, reduceTaskNumber))
		if err != nil {
			return
		}
		dec := json.NewDecoder(file)
		for dec.More() {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				continue
			}
			kvList = append(kvList, kv)
		}
	}

	sort.Sort(kvList)
	wFile, err := os.OpenFile(outFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer wFile.Close()
	if len(kvList) == 0 {
		return
	}
	key := kvList[0].Key
	values := []string{kvList[0].Value}
	enc := json.NewEncoder(wFile)
	for _, kv := range kvList[1:] {
		if kv.Key != key {
			enc.Encode(KeyValue{key, reduceF(key, values)})
			key = kv.Key
			values = []string{kv.Value}
			continue
		}
		values = append(values, kv.Value)
	}
	enc.Encode(KeyValue{key, reduceF(key, values)})

}

type KvList []KeyValue

func (kv KvList) Len() int      { return len(kv) }
func (kv KvList) Swap(i, j int) { kv[i], kv[j] = kv[j], kv[i] }
func (kv KvList) Less(i, j int) bool {
	return kv[i].Key < kv[j].Key
}
