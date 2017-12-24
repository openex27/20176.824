package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"time"
)
import (
	"labrpc"
	"sync/atomic"
)

// import "bytes"
// import "encoding/gob"

const (
	HBI = 150
	LCT = 350
	FMI = 300
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
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
	Log  interface{}
	Term int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
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

	inCatch   []sync.Mutex
	startChan chan struct{}
	commitChan chan struct{}
	//quitVote chan struct{}
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

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
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term        int
	CandidateId int
	// Your data here (2A, 2B).
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
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
		if args.LastLogTerm >= rf.logs[rf.lastApplied].Term && args.LastLogIndex >= rf.lastApplied {
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.Term = args.Term
			reply.VoteGranted = true
			atomic.StoreInt32(&rf.isFollow, int32(1))
		} else {
			DPrintf("no permise")
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
		}
	} else if rf.votedFor == -1 { // RequestVote RPC : Receiver implementation 2(part)
		if args.LastLogTerm >= rf.logs[rf.lastApplied].Term && args.LastLogIndex >= rf.lastApplied {
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.Term = args.Term
			reply.VoteGranted = true
			atomic.StoreInt32(&rf.isFollow, int32(1))
		} else {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
		}
	} else { // a.T == r.cT and r.vF != -1 #voted some one in same term
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
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
		LastLogTerm:  rf.logs[rf.lastApplied].Term,
	}
	DPrintf("start new voting i am %d  term = %d vf:=%d", rf.me,rf.currentTerm,rf.votedFor)
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
				DPrintf("me=%d, voter=%d\n", rf.me, who)
				return
			} else {
				rf.mu.Lock()
				if rf.currentTerm < reply.Term { //切换至follow,通过修改currentTerm抑制获得major后升级
					rf.currentTerm = reply.Term
					atomic.StoreInt32(&rf.isFollow, int32(1))
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
				//DPrintf("success = %d\n", success)
				if !upLevel && success > half {
					upLevel = true
					rf.mu.Lock()
					if rf.currentTerm > oldTerm { //这轮选举已经结束,无法升级
						rf.mu.Unlock()
						continue
					} else {
						if rf.votedFor == -1 {
							rf.votedFor = rf.me
							rf.isLeader = int32(1)
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
		ms := rand.Int() % 150
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
			ms := rand.Int() % 150
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
	Entries      interface{}
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var newIndex int
	//Figure 2:AppendEntries 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Entries == nil {
		goto HeartBeat
	}
	//Figure 2:AppendEntries 2
	if args.PrevLogIndex > rf.lastApplied || args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	newIndex = args.PrevLogIndex + 1
	if newIndex <= rf.lastApplied {
		if rf.logs[newIndex].Term == args.Term { //Figure 2:AppendEntries 4
			rf.lastApplied = newIndex
		} else { //Figure 2:AppendEntries 3
			rf.logs[newIndex].Term = args.Term
			rf.logs[newIndex].Log = args.Entries
			rf.lastApplied = newIndex
		}
	} else {
		rf.logs = append(rf.logs, logEntry{
			Log:  args.Entries,
			Term: args.Term,
		})
		rf.lastApplied++
	}

	if args.LeaderCommit > rf.commitIndex {
		var minCommit int
		if rf.lastApplied < args.LeaderCommit {
			minCommit = rf.lastApplied
		} else {
			minCommit = args.LeaderCommit
		}
		for i := rf.commitIndex + 1; i <= minCommit; i++ {
			rf.applyCh <- ApplyMsg{
				Index:   i,
				Command: rf.logs[i].Log,
			}
		}
		rf.commitIndex = minCommit
	}
HeartBeat:
	reply.Term = args.Term
	reply.Success = true
	if rf.me == args.LeaderId {
		return
	}
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId //(not only)变成follower
	atomic.StoreInt32(&rf.isFollow, int32(1))
	atomic.StoreInt32(&rf.isLeader,int32(0))
}

func (rf *Raft) sendHeartbeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type leaderBand struct {
	sync.Mutex
	Expired bool
}

func (rf *Raft) beginHeartbeat() {
	rf.mu.Lock()
	all := len(rf.peers)
	oldTerm := rf.currentTerm
	args := AppendEntriesArgs{
		Term:     oldTerm,
		LeaderId: rf.me,
		Entries:  nil,
	}
	rf.mu.Unlock()
	go rf.autoCommit()
	go rf.catchUper()
	recvAppend := make(chan int, 5)

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
			rf.mu.Unlock()
			wg.Add(all)
			DPrintf("heartbeat")
			for i := 0; i < all; i++ {
				go func(who int) {
					reply := new(AppendEntriesReply)

					if ok := rf.sendHeartbeat(who, &args, reply); ok {
						if reply.Success {
							recvAppend <- who
							wg.Done()
						} else {
							wg.Done()
							rf.mu.Lock()
							if rf.currentTerm < reply.Term {
								rf.currentTerm = reply.Term
								rf.votedFor = -2
								rf.mu.Unlock()
								atomic.StoreInt32(&rf.isFollow, int32(1))
								atomic.StoreInt32(&rf.isLeader,int32(0))
								DPrintf("giveup")
							} else {
								rf.mu.Unlock()
							}
						}
					}
				}(i)
			}
		}
	}()

	go func() { //心跳接收,判断是否leader失效
		major := all/2 + 1
		firstLB := leaderBand{
			Expired: false,
		}
		var lBP *leaderBand
		lBP = &firstLB
		done := make(chan struct{}, 1)
		once := sync.Once{}
		sendDoneFunc := func() {
			done <- struct{}{}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.currentTerm > oldTerm || rf.votedFor == -1 {
				return
			}

			rf.votedFor = -1
			go rf.beginVote(rf.currentTerm)
			DPrintf("\noutline\n\n")
		}
		go func(lb *leaderBand) {
			time.Sleep(LCT * time.Millisecond)
			lb.Lock()
			defer lb.Unlock()
			if !lb.Expired { //放权,切换到候选者模式,开始投票
				once.Do(sendDoneFunc)
				atomic.StoreInt32(&rf.isLeader,int32(0))
			}
		}(lBP)

		markArr := make([]bool, all, all)
		inCount := 0
		for {
			select {
			case <-done:
				go func() { //清空channel防止泄露
					for {
						_, ok := <-recvAppend
						if !ok {
							return
						}

					}
				}()
				return
			case who, ok := <-recvAppend:
				if !ok {
					continue
				}
				if !markArr[who] {
					markArr[who] = true
					inCount++
				}
				if inCount == major {
					inCount = 0
					flushBoolSlice(&markArr)
					lBP.Lock()
					lBP.Expired = true
					lBP.Unlock()
					lBP = new(leaderBand)
					go func(lb *leaderBand) {
						time.Sleep(LCT * time.Millisecond)
						lb.Lock()
						defer lb.Unlock()
						if !lb.Expired { //放权,切换到候选者模式,开始投票
							once.Do(sendDoneFunc)
							atomic.StoreInt32(&rf.isLeader,int32(0))
						}
					}(lBP)
				}
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
	t := time.NewTicker(FMI * time.Millisecond)
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
		go rf.beginVote(rf.currentTerm)
		rf.mu.Unlock()
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
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
		rf.logs = append(rf.logs,logEntry{
			Log:  command,
			Term: rf.currentTerm,
		})
		index, term, isLeader = rf.lastApplied, rf.currentTerm, true
		rf.startChan <- struct{}{}
	}
	// Your code here (2B).
	if isLeader {
		DPrintf("index=%d,term=%d,VoteFor=%d\n", index, term,rf.votedFor)
	}
	return index, term, isLeader
}

func (rf *Raft) catchUp(which int) {
	var preTerm, preIndex int
	var args AppendEntriesArgs
	rf.inCatch[which].Lock()
	defer rf.inCatch[which].Unlock()
	rf.mu.Lock()
	if rf.nextIndex[which] > rf.lastApplied {
		rf.mu.Unlock()
		return
	}
	if rf.logs[rf.nextIndex[which]-1].Term != rf.currentTerm {
		preIndex = rf.lastApplied - 1
		preTerm = rf.logs[preIndex].Term
	} else {
		preIndex = rf.nextIndex[which] - 1
		preTerm = rf.logs[preIndex].Term
	}
	rf.mu.Unlock()
	for {
		rf.mu.Lock()
		if rf.votedFor != rf.me {
			rf.mu.Unlock()
			return
		}
		args = AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: preIndex,
			PrevLogTerm:  preTerm,
			LeaderCommit: rf.commitIndex,
			Entries:      rf.logs[preIndex+1].Log,
		}
		rf.mu.Unlock()
		reply := new(AppendEntriesReply)
		status := rf.sendAppendEntry(which, &args, reply)
		switch status {
		case 0:
			preIndex++

			rf.mu.Lock()
			if rf.nextIndex[which] <= preIndex {
				rf.nextIndex[which]++
			}
			if preIndex == rf.lastApplied {
				rf.mu.Unlock()
				rf.commitChan<- struct{}{}
				return
			}
			preTerm = rf.logs[preIndex].Term
			rf.mu.Unlock()
			rf.commitChan<- struct{}{}
		case 1:
			DPrintf("giveup")
			return
		case 2:
			preIndex, preTerm = rf.findPreIndex(preIndex, preTerm)
		}
	}
}

func (rf *Raft) findPreIndex(nowIndex int, nowTerm int) (int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	position := -1
	if rf.logs[nowIndex-1].Term == nowTerm {
		position = 1 //tail
	} else {
		position = 0 //head
	}
	switch position {
	case 0:
		nowIndex--
		return nowIndex, rf.logs[nowIndex].Term
	case 1:
		nowIndex = nowIndex - 2
		for {
			if rf.logs[nowIndex].Term != nowTerm {
				nowIndex++
				return nowIndex, rf.logs[nowIndex].Term
			}
			nowIndex--
		}
	}
	return nowIndex,nowTerm
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) int {

	for {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if !ok {
			time.Sleep(50*time.Millisecond)
			continue //network fail
		}
		if !reply.Success {
			rf.mu.Lock()
			if rf.currentTerm < reply.Term {
				rf.currentTerm = reply.Term
				//rf.votedFor = -2
				atomic.StoreInt32(&rf.isFollow, int32(1))
				atomic.StoreInt32(&rf.isLeader,int32(0))
				rf.mu.Unlock()
				return 1 // highly term turn to follow
			}
			rf.mu.Unlock()
			return 2 // need decrement preIndex
		}
		return 0
	}
}

func (rf *Raft) catchUper() {
	//tick := time.NewTicker(50 * time.Millisecond)
	for {
		if atomic.LoadInt32(&rf.isLeader)!=int32(1){
			return
		}
		<-rf.startChan
		rf.mu.Lock()
		for follower := 0;follower<len(rf.peers);follower++{
			if follower == rf.me{
				continue
			}
			if rf.nextIndex[follower]<=rf.lastApplied{
				go rf.catchUp(follower)
			}
		}
		rf.mu.Unlock()

	}
}

func (rf *Raft) autoCommit(){
	var args AppendEntriesArgs
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	rf.mu.Unlock()
	for{
		if atomic.LoadInt32(&rf.isLeader)!=int32(1){
			return
		}
		<-rf.commitChan
		rf.mu.Lock()
		if rf.lastApplied == rf.commitIndex{
			rf.mu.Unlock()
			continue
		}
		checkIndex := rf.commitIndex + 1
		count := 1
		for follower:=0;follower<len(rf.peers);follower++{
			if follower == rf.me{
				continue
			}
			if rf.nextIndex[follower]>checkIndex{
				count++
			}
		}
		if count >= len(rf.peers)/2{
			args.PrevLogIndex = rf.commitIndex
			args.LeaderCommit = rf.commitIndex+1
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
			args.Entries = rf.logs[args.PrevLogIndex+1].Log
			rf.commitIndex++
			rf.applyCh <- ApplyMsg{
				Index:   rf.commitIndex,
				Command: rf.logs[rf.commitIndex].Log,
			}
			rf.mu.Unlock()
			for n:=0;n<len(rf.peers);n++{
				if n == rf.me{
					continue
				}
				go func(which int,args *AppendEntriesArgs){
					temp:=new(AppendEntriesReply)
					rf.sendAppendEntry(which,args,temp)
				}(n,&args)
			}
		}else{
			rf.mu.Unlock()
		}
		//DPrintf("Count=%d\n",count)


	}
}

func (rf *Raft) status(){
	t := time.NewTicker(500 * time.Millisecond)
	for {
		<-t.C
		rf.mu.Lock()
		DPrintf("status term=%d  votef=%d  me=%d\n", rf.currentTerm, rf.votedFor, rf.me)
		rf.mu.Unlock()
	}
}


//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -2
	rf.applyCh = applyCh
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.logs = []logEntry{logEntry{
		Term: 0,
	}}
	rf.nextIndex = make([]int, len(peers), len(peers))
	for i:=0;i<len(peers);i++{
		rf.nextIndex[i] =1
	}
	rf.matchIndex = make([]int, len(peers), len(peers))
	// Your initialization code here (2A, 2B, 2C).
	rf.isFollow = int32(0)
	rf.isLeader = int32(0)
	rf.inCatch = make([]sync.Mutex, len(peers), len(peers))
	rf.startChan = make(chan struct{}, 10)
	rf.commitChan = make(chan struct{},10)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.followerMaintain()
	go rf.status()
	return rf
}
