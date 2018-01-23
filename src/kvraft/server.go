package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"


)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Method string
	Key    string
	Value  string
	Uid    int64
	Cid    int64

	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type hashcode struct {
	Client int64
	Uid    int64
}

type callbackMap struct {
	sync.Mutex
	M map[hashcode][]*chan bool
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	KVstate   map[string]string
	CliLastId map[int64]int64

	CallBackChan callbackMap
}

func (kv *RaftKV) HandleApply() {
	for msg := range kv.applyCh {
		op := msg.Command.(Op)
		//DPrintf("recive applyMsg me=%d method = %s",kv.me,op.Method)
		kv.mu.Lock()
		switch op.Method {
		case "Get":
			if op.Uid > kv.CliLastId[op.Cid]{
				kv.CliLastId[op.Cid] = op.Uid
			}
			go kv.sendSignal(op.Cid,op.Uid)
		case "Put":
			if op.Uid > kv.CliLastId[op.Cid]{
				kv.KVstate[op.Key] = op.Value
				kv.CliLastId[op.Cid] = op.Uid
			}
			go kv.sendSignal(op.Cid,op.Uid)
		case "Append":
			if op.Uid > kv.CliLastId[op.Cid]{
				kv.KVstate[op.Key] = kv.KVstate[op.Key]+op.Value
				kv.CliLastId[op.Cid] = op.Uid
			}
			go kv.sendSignal(op.Cid,op.Uid)
		}
		kv.mu.Unlock()
	}
}

func (kv *RaftKV)sendSignal(cid,uid int64){
	kv.CallBackChan.Lock()
	defer kv.CallBackChan.Unlock()
	for _, c := range kv.CallBackChan.M[hashcode{cid, uid}] {
		*c <- true
	}
	delete(kv.CallBackChan.M,hashcode{cid, uid})

}

func (kv *RaftKV) registChan(client, uid int64, c *chan bool) {
	kv.CallBackChan.Lock()
	defer kv.CallBackChan.Unlock()
	tempHC := hashcode{
		client,
		uid,
	}
	kv.CallBackChan.M[tempHC] = append(kv.CallBackChan.M[tempHC], c)
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	c := make(chan bool, 1)
	kv.mu.Lock()
	if LID, ok := kv.CliLastId[args.ClientId]; ok && LID >= args.Uid {
		reply.Value = kv.KVstate[args.Key]
		reply.Err = ""
		kv.mu.Unlock()
		return
	}
	_, _, suc := kv.rf.Start(Op{
		"Get",
		"", // or args.Key?
		"",
		args.Uid,
		args.ClientId,
	})
	if !suc {
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	} else {
		kv.registChan(args.ClientId, args.Uid, &c)
	}
	kv.mu.Unlock()
	//TODO detect leadership change
	t := time.NewTicker(500 * time.Millisecond)
	for{
		select {
		case  <-c:
			kv.mu.Lock()
			reply.Value = kv.KVstate[args.Key]
			kv.mu.Unlock()
			close(c)
			return
			case <-t.C:
				//if _, isLeader := kv.rf.GetState();!isLeader{
					DPrintf("tick")
					reply.WrongLeader = true
					go func(){
						<-c
						close(c)
					}()
					return
				//}
		}
	}

	// Your code here.
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	c := make(chan bool,1)
	kv.mu.Lock()
	if LID, ok := kv.CliLastId[args.Uid]; ok && LID >= args.Uid {
		reply.Err = ""
		kv.mu.Unlock()
		return
	}
	op := Op{
		Key:   args.Key,
		Value: args.Value,
		Uid:   args.Uid,
		Cid:   args.ClientId,
	}
	if args.Op == "Put" || args.Op == "Append" {
		op.Method = args.Op
	} else {
		kv.mu.Unlock()
		reply.Err = "unknow Op"
		return
	}

	_, _, suc := kv.rf.Start(op)
	if !suc {
		reply.WrongLeader = true
		DPrintf("start error %d",kv.me)
		kv.mu.Unlock()
		return
	} else {
		kv.registChan(args.ClientId, args.Uid, &c)
	}
	kv.mu.Unlock()
	t := time.NewTicker(500 * time.Millisecond)
	for{
		select {
		case  <-c:
			reply.WrongLeader = false
			reply.Err = ""
			close(c)
			return
		case <-t.C:
			//if _, isLeader := kv.rf.GetState();!isLeader{
				DPrintf("tick")
				reply.WrongLeader = true
				go func(){
					<-c
					close(c)
				}()
				return
			//}
		}
	}

	// Your code here.
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.CliLastId = make(map[int64]int64)
	kv.KVstate = make(map[string]string)
	kv.CallBackChan = callbackMap{
		M: make(map[hashcode][]*chan bool),
	}
	go kv.HandleApply()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}
