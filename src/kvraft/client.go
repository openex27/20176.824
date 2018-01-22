package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	mu sync.Mutex
	leaderId int
	clientId int64
	Uid int64


	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.Uid = int64(0)
	ck.clientId = nrand()
	ck.leaderId = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	cleader := ck.leaderId
	ck.Uid++
	args := GetArgs{
		Key:key,
		Uid:ck.Uid,
		ClientId:ck.clientId,
	}

	for {
		reply := new(GetReply)
		DPrintf("get %v\n",args)
		if ok := ck.servers[cleader].Call("RaftKV.Get", &args, reply);!ok{
			continue
		}
		if reply.WrongLeader{
			cleader = (cleader+1)%len(ck.servers)
			continue
		}
		ck.leaderId = cleader
		return reply.Value
	}
	// You will have to modify this function.

	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	cleader := ck.leaderId
	ck.Uid++
	args := PutAppendArgs{
		Key:key,
		Value:value,
		Op:op,
		Uid:ck.Uid,
		ClientId:ck.clientId,
	}

	for{
		reply := new(PutAppendReply)
		DPrintf("put %v \n",args)
		if ok:=ck.servers[cleader].Call("RaftKV.PutAppend", &args, reply);!ok{
			//DPrintf("connect %d error",cleader)
			cleader = (cleader+1)%len(ck.servers)
			continue
		}
		if reply.WrongLeader{
			cleader = (cleader+1)%len(ck.servers)
			//DPrintf("change leader %d\n",cleader)
			continue
		}
		ck.leaderId = cleader
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
