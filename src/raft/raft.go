package raft

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)
import (
	"labrpc"
	"runtime"
	"sync/atomic"

	_ "net/http/pprof"
	"fmt"
)

const (
	HBI = 50
	LCT = 400
	FMI = 200 //Follower maintain interval
	RVI = 200
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type logEntry struct {
	Log       interface{}
	Term      int
	Committed bool
}

type Raft struct {
	//mu        sync.Mutex          // Lock to protect shared access to this peer's state
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	currentTerm int
	votedFor    int //-1表示候选人,值为me表示leader,否则为follow

	isFollow int32
	isLeader int32

	logs        []logEntry
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
	applyCh    chan ApplyMsg
	//bufApplyCh chan ApplyMsg

	snapLastIndex int
	snapLastTerm  int
	headIndex     int

	checkSnapshot chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.me == rf.votedFor {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

func (rf *Raft) GetSnapshotSignalChan() chan struct{} {
	return rf.checkSnapshot
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.snapLastIndex)
	e.Encode(rf.snapLastTerm)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	if Debug == 3 {
		DPrintf("write persisted  commitID=%d lastIndex=%d me=%d", rf.commitIndex, rf.lastApplied, rf.me)
	}
}

func (rf *Raft) TrimOldLogs(lastIndex int,threshold int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.persister.RaftStateSize() < threshold || rf.headIndex> lastIndex {
		return
	}
	rf.snapLastIndex = lastIndex
	rf.snapLastTerm = rf.logs[lastIndex-rf.headIndex].Term

	tempLogs := make([]logEntry,len(rf.logs[lastIndex-rf.headIndex+1:]),len(rf.logs))
	copy(tempLogs, rf.logs[lastIndex-rf.headIndex+1:])
	rf.logs = tempLogs
	rf.headIndex = lastIndex + 1
	rf.persist()
	fmt.Printf("me=%d trim %d  log=%v\n",rf.me,lastIndex,rf.logs)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
	d.Decode(&rf.snapLastIndex)
	d.Decode(&rf.snapLastTerm)
	rf.headIndex = rf.snapLastIndex + 1

	logLen := len(rf.logs) - 1
	if rf.snapLastIndex != -1 {
		rf.lastApplied = rf.headIndex + logLen
		rf.applyCh <- ApplyMsg{
			UseSnapshot:true,
			Snapshot:rf.persister.ReadSnapshot(),
		}
	} else {
		rf.lastApplied = logLen
	}

	for {
		if logLen <= 0 {
			break
		}
		if rf.logs[logLen].Committed == true {
			break
		}
		logLen--
	}
	rf.commitIndex = logLen + rf.headIndex
	var i int
	if rf.headIndex == 0 {
		i = 1
	} else {
		i = 0
	}
	for ; i <= rf.commitIndex-rf.headIndex; i++ {
		rf.applyCh <- ApplyMsg{
			Index:   i + rf.headIndex,
			Command: rf.logs[i].Log,
		}
	}
	if Debug == 3 {
		DPrintf("read persisted %v commitID=%d lastIndex=%d me=%d", rf.logs, rf.commitIndex, rf.lastApplied, rf.me)
	}
}

type InstallSnapshotArgs struct {
	Term int
	//leaderId int   //why use leaderID?
	LastIncludedIndex int
	LastIncludedTerm  int

	Data []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.LastIncludedIndex >= rf.lastApplied {
		rf.headIndex = args.LastIncludedIndex + 1
		rf.snapLastTerm = args.LastIncludedTerm
		rf.snapLastIndex = args.LastIncludedIndex
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIndex = args.LastIncludedIndex
		rf.logs = make([]logEntry, 0,8)
		rf.applyCh <- ApplyMsg{
			UseSnapshot: true,
			Snapshot:    args.Data,
		}
		rf.persist()
	} else {
		if rf.headIndex > args.LastIncludedIndex{
			return
		}
		if rf.logs[args.LastIncludedIndex-rf.headIndex].Term == args.LastIncludedTerm && rf.logs[args.LastIncludedIndex-rf.headIndex].Committed {
			return
		} else {
			rf.headIndex = args.LastIncludedIndex + 1
			rf.snapLastTerm = args.LastIncludedTerm
			rf.snapLastIndex = args.LastIncludedIndex
			rf.commitIndex = args.LastIncludedIndex
			tempLog := make([]logEntry, len(rf.logs[args.LastIncludedIndex-rf.headIndex+1:]),len(rf.logs))
			copy(tempLog, rf.logs[args.LastIncludedIndex-rf.headIndex+1:])
			rf.logs = tempLog
			rf.applyCh <- ApplyMsg{
				UseSnapshot: true,
				Snapshot:    args.Data,
			}
			rf.persist()
		}
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.CandidateId == rf.me {
		reply.VoteGranted = true
		return
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		var localTerm int
		if rf.headIndex > rf.lastApplied{
			localTerm = rf.snapLastTerm
		}else{
			localTerm = rf.logs[rf.lastApplied-rf.headIndex].Term
		}
		if args.LastLogTerm > localTerm {
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.Term = args.Term
			reply.VoteGranted = true
			atomic.StoreInt32(&rf.isFollow, int32(1))
			atomic.StoreInt32(&rf.isLeader, int32(0))
			rf.persist()
		} else if args.LastLogTerm == localTerm && args.LastLogIndex >= rf.lastApplied {
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.Term = args.Term
			reply.VoteGranted = true
			atomic.StoreInt32(&rf.isFollow, int32(1))
			atomic.StoreInt32(&rf.isLeader, int32(0))
			rf.persist()
		} else {
			//	DPrintf("no permise am %d sender %d senderTerm %d\n", rf.me, args.CandidateId, args.Term)
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			rf.currentTerm = args.Term
			rf.votedFor = -1
			atomic.StoreInt32(&rf.isFollow, int32(0))
			atomic.StoreInt32(&rf.isLeader, int32(0))

			time.Sleep(time.Duration(rand.Int()%100) * time.Millisecond)
			go rf.beginVote(rf.currentTerm)
		}
	} else if rf.votedFor == -1 || rf.votedFor == -2 { // RequestVote RPC : Receiver implementation 2(part)
		var localTerm int
		if rf.headIndex > rf.lastApplied{
			localTerm = rf.snapLastTerm
		}else{
			localTerm = rf.logs[rf.lastApplied-rf.headIndex].Term
		}
		if args.LastLogTerm > localTerm {
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.Term = args.Term
			reply.VoteGranted = true
			atomic.StoreInt32(&rf.isFollow, int32(1))
			atomic.StoreInt32(&rf.isLeader, int32(0))
			rf.persist()
		} else if args.LastLogTerm == localTerm && args.LastLogIndex >= rf.lastApplied {
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.Term = args.Term
			reply.VoteGranted = true
			atomic.StoreInt32(&rf.isFollow, int32(1))
			atomic.StoreInt32(&rf.isLeader, int32(0))
			rf.persist()
		} else {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
		}
	} else { // a.T == r.cT and r.vF != -1 #voted some one in same term
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) beginOnceVote(oldTerm int) {
	voteRetChan := make(chan bool, 1)

	rf.mu.Lock()
	if rf.votedFor != -1 || rf.currentTerm > oldTerm {
		rf.mu.Unlock()
		return
	}

	args := RequestVoteArgs{
		Term:         oldTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastApplied,
	//	LastLogTerm:  rf.logs[rf.lastApplied-rf.headIndex].Term,
	}

	if (rf.lastApplied-rf.headIndex) == -1{
		args.LastLogTerm = rf.snapLastTerm
	}else{
		args.LastLogTerm = rf.logs[rf.lastApplied-rf.headIndex].Term
	}
	//DPrintf("start new voting i am %d  term = %d vf:=%d", rf.me, rf.currentTerm, rf.votedFor)
	rf.mu.Unlock()
	//发送所有voteRPC
	for i := 0; i < len(rf.peers); i++ {
		go func(who int) {
			reply := new(RequestVoteReply)
			if ok := rf.sendRequestVote(who, &args, reply); !ok {
				voteRetChan <- false
				return
			}
			if reply.VoteGranted {
				voteRetChan <- true
				//DPrintf("me=%d, voter=%d\n", rf.me, who)
				return
			} else {
				rf.mu.Lock()
				if rf.currentTerm < reply.Term { //切换至follow,通过修改currentTerm抑制获得major后升级
					rf.currentTerm = reply.Term
					atomic.StoreInt32(&rf.isFollow, int32(1))
					atomic.StoreInt32(&rf.isLeader, int32(0))
					rf.persist()
				}
				rf.mu.Unlock()
				voteRetChan <- false
			}
		}(i)
	}
	//接收所有RPC结果
	go func() {
		count := 0
		success := 0
		rf.mu.Lock()
		all := len(rf.peers)
		half := all / 2
		rf.mu.Unlock()
		upLevel := false
		for ack := range voteRetChan {
			count++
			if count == all {
				close(voteRetChan)
			}
			if ack {
				success++
				if !upLevel && success > half {
					upLevel = true
					rf.mu.Lock()
					if rf.currentTerm > oldTerm { //这轮选举已经结束,无法升级
						rf.mu.Unlock()
						continue
					} else {
						if rf.votedFor == -1 {
							rf.votedFor = rf.me
							atomic.StoreInt32(&rf.isLeader, int32(1))
							DPrintf("be leader %v LA=%d LC=%d me=%d", rf.logs, rf.lastApplied, rf.commitIndex, rf.me)
							rf.nextIndex = make([]int, all, all)
							//tempNext := rf.lastApplied
							for i := 0; i < all; i++ {
								rf.nextIndex[i] = rf.lastApplied + 1
							}
							rf.matchIndex = make([]int, all, all)
							go rf.beginHeartbeat()
						}
						rf.mu.Unlock()
					}
				}
			}
		}
	}()
}

//BeginVote 状态成为候选者,发起选举
func (rf *Raft) beginVote(oldTerm int) {
	ticker := make(chan struct{})
	rf.mu.Lock()
	if rf.currentTerm > oldTerm {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm++
	oldTerm = rf.currentTerm
	go rf.beginOnceVote(oldTerm)
	rf.mu.Unlock()
	go func() {
		ms := rand.Int()%100 + RVI
		time.Sleep(time.Duration(ms)*time.Millisecond + time.Duration(150))
		ticker <- struct{}{}
	}()
	for {
		<-ticker
		rf.mu.Lock()
		if rf.votedFor != -1 || rf.currentTerm > oldTerm {
			rf.mu.Unlock()
			return
		}
		rf.currentTerm++
		oldTerm = rf.currentTerm
		go rf.beginOnceVote(oldTerm)
		rf.mu.Unlock()

		go func() {
			ms := rand.Int()%200 + RVI
			time.Sleep(time.Duration(ms)*time.Millisecond + time.Duration(150))
			ticker <- struct{}{}
		}()
	}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []logEntry
	//EntriesTerm  int

}

type AppendEntriesReply struct {
	Term        int
	Success     bool
	MyNextIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//TODO quicly relpy request when handing some other append
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//var newIndex int
	if rf.me == args.LeaderId {
		reply.Term = args.Term
		reply.Success = true
		return
	}
	//Figure 2:AppendEntries 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	reply.Term = rf.currentTerm
	atomic.StoreInt32(&rf.isFollow, int32(1))
	atomic.StoreInt32(&rf.isLeader, int32(0))
	if args.Entries == nil {
		goto Committing
	}

	//Figure 2:AppendEntries 2
	if args.PrevLogIndex > rf.lastApplied {
		//reply.Term = rf.currentTerm
		reply.Success = false
		reply.MyNextIndex = rf.lastApplied + 1
		return
	} else {
		realIndex := args.PrevLogIndex - rf.headIndex
		switch {
		case realIndex == -1:
			if args.PrevLogTerm != rf.snapLastTerm {
				reply.Success = false
				reply.MyNextIndex = -1
				return
			}
		case realIndex < -1:
			reply.Success = false
			reply.MyNextIndex = rf.headIndex
			return
		default:
			if args.PrevLogTerm != rf.logs[realIndex].Term {
				//reply.Term = rf.currentTerm
				reply.Success = false
				reply.MyNextIndex = rf.findMyNextIndex(realIndex)
				return
			}
		}

	}

	rf.logs = rf.logs[:args.PrevLogIndex-rf.headIndex+1] //TODO
	rf.logs = append(rf.logs, args.Entries...)
	rf.lastApplied = args.PrevLogIndex + len(args.Entries)

Committing:
	reply.MyNextIndex = rf.lastApplied + 1
	//reply.Term = args.Term
	reply.Success = true
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId //(not only)变成follower

	if args.LeaderCommit > rf.commitIndex {
		var minCommit int
		if rf.lastApplied < args.LeaderCommit {
			minCommit = rf.lastApplied
		} else {
			minCommit = args.LeaderCommit
		}
		//DPrintf("trace = %d len = %d la=%d lc=%d",minCommit,len(rf.logs),rf.lastApplied,args.LeaderCommit)
		var localTerm int
		if rf.headIndex > minCommit{
			localTerm = rf.snapLastTerm
		}else{
			localTerm = rf.logs[minCommit-rf.headIndex].Term
		}
		if localTerm == args.Term {
			for i := rf.commitIndex + 1; i <= minCommit; i++ {
				tempApply := ApplyMsg{
					Index:   i,
					Command: rf.logs[i-rf.headIndex].Log,
				}
				//rf.mu.Unlock()
				rf.applyCh <- tempApply
				//rf.mu.Lock()
				DPrintf("follower commit value me=%d log[%d]=%v ", rf.me, i, rf.logs[i-rf.headIndex].Log)
				rf.logs[i-rf.headIndex].Committed = true
			}
			rf.commitIndex = minCommit
			rf.persist()
			go func() {
				rf.checkSnapshot <- struct{}{}
			}()
		}
	}

}

func (rf *Raft) findMyNextIndex(nowIndex int) (myNextInext int) {
	nowTerm := rf.logs[nowIndex].Term

	for nowIndex--; nowIndex >= 0 && rf.logs[nowIndex].Term == nowTerm; nowIndex-- {
	}
	if rf.headIndex == 0 {
		if nowIndex < 0 {
			return 1
		} else {
			return nowIndex + 1
		}
	} else {
		if nowIndex < 0 {
			return rf.headIndex + 1
		} else {
			return nowIndex + rf.headIndex + 1
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) beginHeartbeat() {
	rf.mu.Lock()
	all := len(rf.peers)
	oldTerm := rf.currentTerm
	args := AppendEntriesArgs{
		Term:     oldTerm,
		LeaderId: rf.me,
		Entries:  nil,
		//PrevLogIndex: -1,
	}
	rf.mu.Unlock()
	go rf.autoCommit()
	//go rf.catchUper()
	recvAppend := make(chan int)

	go func() { //以HBI为周期发送心跳给各follow
		t := time.NewTicker(HBI * time.Millisecond)
		wg := sync.WaitGroup{}
		for {
			<-t.C
			rf.mu.Lock()
			if rf.votedFor == -1 || rf.currentTerm > oldTerm {
				rf.mu.Unlock()
				wg.Wait()
				close(recvAppend)
				return
			}
			args.LeaderCommit = rf.commitIndex
			rf.mu.Unlock()
			wg.Add(all - 1)
			for i := 0; i < all; i++ {
				if i == rf.me {
					continue
				}
				go func(who int, args AppendEntriesArgs) {
					rf.mu.Lock()
					if rf.votedFor != rf.me {
						rf.mu.Unlock()
						wg.Done()
						return
					}
					if rf.nextIndex[who] < rf.headIndex {
						snapArgs := InstallSnapshotArgs{
							Term:              oldTerm,
							LastIncludedIndex: rf.snapLastIndex,
							LastIncludedTerm:  rf.snapLastTerm,
							Data:              rf.persister.ReadSnapshot(),
						}
						rf.mu.Unlock()
						reply := new(InstallSnapshotReply)
						if ok := rf.sendInstallSnapshot(who, &snapArgs, reply); ok {
							rf.mu.Lock()
							if rf.currentTerm < reply.Term {
								rf.currentTerm = reply.Term
								rf.votedFor = -2
								atomic.StoreInt32(&rf.isFollow, int32(1))
								atomic.StoreInt32(&rf.isLeader, int32(0))
							} else {
								rf.nextIndex[who] = rf.headIndex
								rf.matchIndex[who] = rf.snapLastIndex
							}
							rf.mu.Unlock()
						}

						return
					}
					args.LeaderCommit = rf.commitIndex
					if rf.nextIndex[who] <= rf.lastApplied {
						preIndex := rf.nextIndex[who] - rf.headIndex - 1
						if preIndex >= 0 {
							args.PrevLogTerm = rf.logs[preIndex].Term
							args.PrevLogIndex = preIndex + rf.headIndex
							args.Entries = make([]logEntry, len(rf.logs[preIndex+1:]))
							copy(args.Entries, rf.logs[preIndex+1:])
						} else {
							args.PrevLogIndex = rf.snapLastIndex
							args.PrevLogTerm = rf.snapLastTerm
							args.Entries = make([]logEntry, len(rf.logs))
							copy(args.Entries, rf.logs)
						}
					}
					rf.mu.Unlock()
					reply := new(AppendEntriesReply)
					if ok := rf.sendAppendEntries(who, &args, reply); ok {
						if reply.Success {
							recvAppend <- who
							wg.Done()
							if args.Entries != nil {
								rf.mu.Lock()
								rf.matchIndex[who] = reply.MyNextIndex - 1
								rf.nextIndex[who] = reply.MyNextIndex
								rf.mu.Unlock()
							}
						} else {
							wg.Done()
							rf.mu.Lock()
							if rf.currentTerm < reply.Term {
								rf.currentTerm = reply.Term
								rf.votedFor = -2
								rf.mu.Unlock()
								atomic.StoreInt32(&rf.isFollow, int32(1))
								atomic.StoreInt32(&rf.isLeader, int32(0))
							} else {
								if args.Entries != nil {
									rf.nextIndex[who] = reply.MyNextIndex
								}
								rf.mu.Unlock()
							}
						}
					} else {
						wg.Done()
					}
				}(i, args)
			}
		}
	}()

	go func() {
		major := all/2 + 1
		count := 1
		markArr := make([]bool, all, all)
		lctTimer := time.NewTimer(LCT * time.Millisecond)
		chandrain := func() { //清空channel防止泄露
			for {
				_, ok := <-recvAppend
				if !ok {
					return
				}
			}
		}
		for {
			select {
			case <-lctTimer.C:
				lctTimer.Stop()
				rf.mu.Lock()
				defer rf.mu.Unlock()
				go chandrain()
				if rf.currentTerm > oldTerm || rf.votedFor == -1 {
					return
				}
				rf.votedFor = -2
				atomic.StoreInt32(&rf.isFollow, int32(1))
				atomic.StoreInt32(&rf.isLeader, int32(0))
				DPrintf("heartbreak by timeout")
				return

			case who, ok := <-recvAppend:
				if !ok {
					if !lctTimer.Stop() {
						<-lctTimer.C
					}
					go chandrain()
				}
				if markArr[who] {
					continue
				}
				count++
				if count == major {
					if !lctTimer.Stop() {
						<-lctTimer.C
					}
					lctTimer.Reset(LCT * time.Millisecond)
					flushBoolSlice(&markArr)
					count = 1
					continue
				}
				markArr[who] = true
			}
		}
	}()
}

func flushBoolSlice(s *[]bool) {
	l := len(*s)
	for i := 0; i < l; i++ {
		(*s)[i] = false
	}
}

func (rf *Raft) followerMaintain() {
	t := time.NewTicker(time.Duration(rand.Int()%150+FMI) * time.Millisecond)
	for {
		<-t.C
		followerStatus := atomic.LoadInt32(&rf.isFollow)
		if followerStatus == int32(1) {
			atomic.StoreInt32(&rf.isFollow, int32(0))
			//DPrintf("%d am follower\n", rf.me)
			continue
		}
		rf.mu.Lock()
		if rf.votedFor == -1 || rf.votedFor == rf.me {
			//DPrintf("%d am not follower\n", rf.me)
			rf.mu.Unlock()
			continue
		}
		rf.votedFor = -1
		//DPrintf("begin vote am %d\n", rf.me)
		go rf.beginVote(rf.currentTerm)
		rf.mu.Unlock()
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.votedFor != rf.me {
		isLeader = false
	} else {
		rf.lastApplied++
		//	DPrintf("index: %d\n",rf.lastApplied)
		rf.logs = append(rf.logs, logEntry{
			Log:  command,
			Term: rf.currentTerm,
		})
		rf.nextIndex[rf.me] = rf.lastApplied + 1
		index, term, isLeader = rf.lastApplied, rf.currentTerm, true
		rf.persist()

	}
	return index, term, isLeader
}

func (rf *Raft) autoCommit() {
	defer DPrintf("autoCommit Done")
	var args AppendEntriesArgs
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	sumPeer := len(rf.peers)
	catchIndex := rf.lastApplied
	if rf.headIndex > 0{
		catchIndex = rf.headIndex
	}
	rf.mu.Unlock()

	catched := false
	tick := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		//	case <-rf.commitChan:
		case <-tick.C:
		}
	fastCommit:
		if atomic.LoadInt32(&rf.isLeader) != int32(1) {
			return
		}
		rf.mu.Lock()
		if rf.lastApplied == rf.commitIndex {
			rf.mu.Unlock()
			continue
		}
		if catched == false {
			if catchIndex > rf.lastApplied {
				goto END
			}
			if rf.logs[catchIndex-rf.headIndex].Term != rf.currentTerm {
				catchIndex++
			} else {
				inCount := 1
				for follower := 0; follower < sumPeer; follower++ {
					if follower == rf.me {
						continue
					}
					if rf.matchIndex[follower] >= catchIndex {
						inCount++
					}
				}
				if inCount > sumPeer/2 {
					catched = true
					rf.mu.Unlock()
					goto fastCommit
				}
			}
		END:
			rf.mu.Unlock()
			continue
		}
		checkIndex := rf.commitIndex + 1
		count := 1
		for follower := 0; follower < sumPeer; follower++ {
			if follower == rf.me {
				continue
			}
			if rf.matchIndex[follower] >= checkIndex {
				count++
			}
		}
		if count > sumPeer/2 {
			rf.commitIndex = checkIndex
			tempApply := ApplyMsg{
				Index:   checkIndex,
				Command: rf.logs[checkIndex-rf.headIndex].Log,
			}
			rf.applyCh <- tempApply
			rf.logs[checkIndex-rf.headIndex].Committed = true
			DPrintf("commit value me=%d log[%d]=%v ", rf.me, checkIndex, rf.logs[checkIndex-rf.headIndex].Log)
			rf.persist()
			go func() {
				rf.checkSnapshot <- struct{}{}
			}()
			rf.mu.Unlock()
			goto fastCommit
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) status(ccc int32) {
	if Debug != 2 {
		return
	}
	t := time.NewTicker(300 * time.Millisecond)
	for {
		<-t.C
		if atomic.LoadInt32(&cc)-int32(3) > ccc {
			return
		}
		DPrintf("goroutince = %d ", runtime.NumGoroutine())
		rf.mu.Lock()
		if rf.votedFor == rf.me {
			DPrintf("Leader: %v matchID=%d leaderCID=%d me=%d", rf.logs, rf.matchIndex, rf.commitIndex, rf.me)
		} else if rf.votedFor == -1 {
			DPrintf("Candidate: voteFor=%d term=%d %v commitID=%d lastIndex=%d me=%d %v %v", rf.votedFor, rf.currentTerm, rf.logs, rf.commitIndex, rf.lastApplied, rf.me, rf.isLeader, rf.isFollow)
		} else {
			DPrintf("Follower: voteFor=%d term=%d %v commitID=%d lastIndex=%d me=%d %v %v", rf.votedFor, rf.currentTerm, rf.logs, rf.commitIndex, rf.lastApplied, rf.me, rf.isLeader, rf.isFollow)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) Kill() {
}

var cc = int32(0)

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	defer DPrintf("make raft %v", cc)
	ccc := atomic.AddInt32(&cc, 1)
	rf := &Raft{}
	rf.peers = peers
	sumPeers := len(peers)
	rf.persister = persister
	rf.me = me
	rf.snapLastIndex = -1
	rf.headIndex = 0
	rf.votedFor = -2
	rf.applyCh = applyCh
	//	rf.bufApplyCh = make(chan ApplyMsg, 10)
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.logs = []logEntry{logEntry{
		Term: 0,
	}}
	rf.nextIndex = make([]int, sumPeers, sumPeers)
	rf.checkSnapshot = make(chan struct{})
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, sumPeers, sumPeers)
	// Your initialization code here (2A, 2B, 2C).
	rf.isFollow = int32(0)
	rf.isLeader = int32(0)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.followerMaintain()
	go rf.status(ccc)
	return rf
}
