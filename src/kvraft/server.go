package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
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
	Key string
	Value string
	Uid int64
	Cid int64

	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type callbackMap struct{
	sync.Mutex
	M map[int64][]chan bool
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	KVstate map[string]string
	CliLastId map[int64]int64

	CallBackChan callbackMap

}

func (kv *RaftKV) HandleApply(){
	for msg := range kv.applyCh{
		op := msg.Command.(Op)
		kv.mu.Lock()
		//TODO  finish apply handler

	}
}

// todo callback need adjust
func (kv *RaftKV) registChan (id int64,c chan bool){
	kv.CallBackChan.Lock()
	defer kv.CallBackChan.Unlock()
	kv.CallBackChan.M[id] = append(kv.CallBackChan.M[id],c)
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	if _,isLeader := kv.rf.GetState();!isLeader{
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	c := make(chan bool)
	kv.mu.Lock()
	if LID , ok := kv.CliLastId[args.ClientId];ok&&LID>=args.Uid{
		reply.Value = kv.KVstate[args.Key]
		reply.Err = ""
		kv.mu.Unlock()
		return
	}
	_,_,suc := kv.rf.Start(Op{
		"GET",
		"",// or args.Key?
		"",
		args.Uid,
		args.ClientId,
	})
	if !suc{
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}else{
		kv.registChan(args.Uid,c)
	}
	kv.mu.Unlock()

	if ok := <-c;ok{
		kv.mu.Lock()
		reply.Value = kv.KVstate[args.Key]
		kv.mu.Unlock()
	}
	return
	// Your code here.
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if _,isLeader := kv.rf.GetState();!isLeader{
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	c := make(chan bool)
	kv.mu.Lock()
	if LID, ok := kv.CliLastId[args.Uid];ok&&LID>=args.Uid{
		reply.Err = ""
		kv.mu.Unlock()
		return
	}
	op := Op{
		Key:args.Key,
		Value:args.Value,
		Uid:args.Uid,
		Cid:args.ClientId,
	}
	if args.Op == "PUT" || args.Op == "APPEND"{
		op.Method = args.Op
	}else{
		kv.mu.Unlock()
		reply.Err = "unknow Op"
		return
	}
	_,_,suc:= kv.rf.Start(op)
	if !suc{
		reply.WrongLeader = true
		kv.mu.Unlock()
		return
	}else{
		kv.registChan(args.Uid,c)
	}
	kv.mu.Unlock()
	if ok := <-c;ok{
		reply.Err = ""
		return
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
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.CliLastId = make(map[int64]int64)
	kv.KVstate = make(map[string]string)
	kv.CallBackChan = callbackMap{
		M:make(map[int64][]chan bool),
	}
	// You may need initialization code here.

	return kv
}
