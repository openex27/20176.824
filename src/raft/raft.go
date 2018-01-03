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
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)
import (
	"labrpc"
	"sync/atomic"
)

const (
	HBI = 100
	LCT = 800
	FMI = 500 //Follower maintain interval
	RVI = 500
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

	inCatch   []int32
	hbf       []int32
	startChan chan struct{}
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

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	if Debug == 3 {
		DPrintf("write persisted  commitID=%d lastIndex=%d me=%d", rf.commitIndex, rf.lastApplied, rf.me)
	}
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
	logLen := len(rf.logs) - 1
	rf.lastApplied = logLen
	for {
		if logLen == 0 {
			break
		}
		if rf.logs[logLen].Committed == true {
			rf.commitIndex = logLen
			break
		}
		logLen--
	}
	if Debug == 3 {
		DPrintf("read persisted %v commitID=%d lastIndex=%d me=%d", rf.logs, rf.commitIndex, rf.lastApplied, rf.me)
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
		if args.LastLogTerm > rf.logs[rf.lastApplied].Term {
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.Term = args.Term
			reply.VoteGranted = true
			atomic.StoreInt32(&rf.isFollow, int32(1))
			atomic.StoreInt32(&rf.isLeader, int32(0))
			rf.persist()
		} else if args.LastLogTerm == rf.logs[rf.lastApplied].Term && args.LastLogIndex >= rf.lastApplied {
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
		if args.LastLogTerm > rf.logs[rf.lastApplied].Term {
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.Term = args.Term
			reply.VoteGranted = true
			atomic.StoreInt32(&rf.isFollow, int32(1))
			atomic.StoreInt32(&rf.isLeader, int32(0))
			rf.persist()
		} else if args.LastLogTerm == rf.logs[rf.lastApplied].Term && args.LastLogIndex >= rf.lastApplied {
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
		LastLogTerm:  rf.logs[rf.lastApplied].Term,
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
		ms := rand.Int()%200 + RVI
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
	atomic.StoreInt32(&rf.isFollow, int32(1))
	atomic.StoreInt32(&rf.isLeader, int32(0))
	if args.PrevLogIndex == -1 {
		goto Committing
	}

	//Figure 2:AppendEntries 2
	if args.PrevLogIndex > rf.lastApplied || args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.MyNextIndex = rf.findMyNextIndex(args.PrevLogIndex)
		return
	}

	rf.logs = rf.logs[:args.PrevLogTerm+1]
	rf.logs = append(rf.logs, args.Entries...)
	rf.lastApplied = args.PrevLogIndex + len(args.Entries)
	reply.MyNextIndex = rf.lastApplied + 1
	/*
		newIndex = args.PrevLogIndex + 1
		if newIndex <= rf.lastApplied {
			if rf.logs[newIndex].Term == args.EntriesTerm { //Figure 2:AppendEntries 4
				//rf.lastApplied = newIndex
				//rf.logs = rf.logs[:newIndex+1]
			} else { //Figure 2:AppendEntries 3
				rf.logs[newIndex].Term = args.EntriesTerm
				rf.logs[newIndex].Log = args.Entries
				rf.lastApplied = newIndex
				rf.logs = rf.logs[:rf.lastApplied+1]
				rf.persist()
				DPrintf("persist 2")
			}
		} else {
			rf.lastApplied++
			rf.logs = append(rf.logs, logEntry{
				Log:  args.Entries,
				Term: args.EntriesTerm,
			})
			rf.persist()
			DPrintf("persist 3")
		}
	*/
Committing:
	reply.Term = args.Term
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
		if rf.logs[minCommit].Term == args.Term {
			for i := rf.commitIndex + 1; i <= minCommit; i++ {
				DPrintf("i=%d len=%d", i, len(rf.logs))
				rf.applyCh <- ApplyMsg{
					Index:   i,
					Command: rf.logs[i].Log,
				}
				DPrintf("follower commit value me=%d log[%d]=%d ", rf.me, i, rf.logs[i].Log)
				rf.logs[i].Committed = true
			}
			rf.commitIndex = minCommit
		}
	}
	rf.persist()
}

func (rf *Raft) findMyNextIndex(nowIndex int) (myNextInext int) {
	if nowIndex > rf.lastApplied {
		return rf.lastApplied + 1
	}
	nowTerm := rf.logs[nowIndex].Term

	for nowIndex--; nowIndex > 0 && rf.logs[nowIndex].Term == nowTerm; nowIndex-- {
	}
	if nowIndex > 0 {
		myNextInext = nowIndex + 1
		return
	} else {
		myNextInext = 1
		return
	}
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
		Term:         oldTerm,
		LeaderId:     rf.me,
		Entries:      nil,
		PrevLogIndex: -1,
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
			args.LeaderCommit = rf.commitIndex
			rf.mu.Unlock()
			for i := 0; i < all; i++ {
				if hbf := atomic.LoadInt32(&rf.hbf[i]); hbf != int32(0) {
					continue
				}
				wg.Add(1)
				go func(who int, args AppendEntriesArgs) {
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
								atomic.StoreInt32(&rf.isLeader, int32(0))
							} else {
								rf.mu.Unlock()
							}
						}
					}
				}(i, args)
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
			rf.votedFor = -2
			atomic.StoreInt32(&rf.isFollow, int32(1))
			atomic.StoreInt32(&rf.isLeader, int32(0))
			DPrintf("heartbreak by timeout")
			//go rf.beginVote(rf.currentTerm)

		}
		go func(lb *leaderBand) {
			time.Sleep(LCT * time.Millisecond)
			lb.Lock()
			defer lb.Unlock()
			if !lb.Expired { //放权,切换到候选者模式,开始投票
				once.Do(sendDoneFunc)
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
	t := time.NewTicker(time.Duration(rand.Int()%300+FMI) * time.Millisecond)
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

func (rf *Raft) catchUp(which int) {
	defer DPrintf("catchUp %d  Done", which)
	var preTerm, preIndex int
	var args AppendEntriesArgs
	defer atomic.StoreInt32(&rf.inCatch[which], int32(0))

	//defer rf.mu.Unlock()
	if rf.nextIndex[which] > rf.lastApplied {
		return
	}
	rf.mu.Lock()
	preIndex = rf.nextIndex[which] - 1
	preTerm = rf.logs[preIndex].Term
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
		}
		args.Entries = make([]logEntry, len(rf.logs[preIndex+1:]))
		copy(args.Entries, rf.logs[preIndex+1:])

		rf.mu.Unlock()
		reply := new(AppendEntriesReply)
		status := rf.sendAppendEntry(which, &args, reply)
		switch status {
		case -1:
			return
		case 0:
			rf.mu.Lock()
			rf.matchIndex[which] = reply.MyNextIndex - 1
			rf.nextIndex[which] = reply.MyNextIndex
			rf.mu.Unlock()
			return

			//rf.commitChan <- struct{}{}
		case 1:
			rf.mu.Lock()
			if rf.currentTerm < reply.Term {
				rf.currentTerm = reply.Term
				rf.votedFor = -2
				atomic.StoreInt32(&rf.isFollow, int32(1))
				atomic.StoreInt32(&rf.isLeader, int32(0))
			}
			rf.mu.Unlock()
			return
		case 2:
			DPrintf("%d reply MyNextIndex = %d", which, reply.MyNextIndex)
			rf.mu.Lock()
			preIndex = reply.MyNextIndex - 1
			preTerm = rf.logs[preIndex].Term
			//rf.nextIndex[which] = reply.MyNextIndex
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) int {
	count := 1
	for {

		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if !ok {
			if count == 5 {
				DPrintf("duang")
				return -1
			}
			count++
			continue //network fail
		}

		if !reply.Success {
			//rf.mu.Lock()
			if args.Term < reply.Term {
				//	rf.mu.Unlock()
				return 1 // highly term turn to follow
			}
			//rf.mu.Unlock()
			atomic.StoreInt32(&rf.hbf[server], int32(1))
			return 2 // need decrement preIndex
		}
		atomic.StoreInt32(&rf.hbf[server], int32(1))
		return 0
	}
}

func (rf *Raft) catchUper() {
	defer DPrintf("out catchUper me=%d\n", rf.me)
	for follower := 0; follower < len(rf.peers); follower++ {
		if follower == rf.me {
			continue
		}
		atomic.StoreInt32(&rf.inCatch[follower], int32(1))
		go rf.catchUp(follower)
	}
	tick := time.NewTicker(50 * time.Millisecond)
	for {
		select {
		case <-rf.startChan:
		case <-tick.C:
		}
		if atomic.LoadInt32(&rf.isLeader) != int32(1) {
			return
		}
		rf.mu.Lock()
		for follower := 0; follower < len(rf.peers); follower++ {
			if follower == rf.me {
				continue
			}
			if rf.nextIndex[follower] <= rf.lastApplied {
				if atomic.LoadInt32(&rf.inCatch[follower]) == int32(0) {
					atomic.StoreInt32(&rf.inCatch[follower], int32(1))
					go rf.catchUp(follower)
				}
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) autoCommit() {
	defer DPrintf("autoCommit Done")
	var args AppendEntriesArgs
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	sumPeer := len(rf.peers)
	catchIndex := rf.lastApplied
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
			if rf.logs[catchIndex].Term != rf.currentTerm {
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
			rf.applyCh <- ApplyMsg{
				Index:   checkIndex,
				Command: rf.logs[checkIndex].Log,
			}
			rf.logs[checkIndex].Committed = true
			DPrintf("commit value me=%d log[%d]=%d ", rf.me, checkIndex, rf.logs[checkIndex].Log)
			rf.persist()
			rf.mu.Unlock()
			goto fastCommit
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) flushHBF() {
	sum := len(rf.peers)
	tick := time.NewTicker(50 * time.Millisecond)
	for {
		<-tick.C
		for i := 0; i < sum; i++ {
			atomic.StoreInt32(&rf.hbf[i], int32(0))
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
	rf.votedFor = -2
	rf.applyCh = applyCh
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.logs = []logEntry{logEntry{
		Term: 0,
	}}
	rf.nextIndex = make([]int, sumPeers, sumPeers)
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, sumPeers, sumPeers)
	// Your initialization code here (2A, 2B, 2C).
	rf.isFollow = int32(0)
	rf.isLeader = int32(0)
	rf.inCatch = make([]int32, sumPeers, sumPeers)
	rf.hbf = make([]int32, sumPeers, sumPeers)
	rf.startChan = make(chan struct{}, 10)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.followerMaintain()
	go rf.status(ccc)
	go rf.flushHBF()
	return rf
}
