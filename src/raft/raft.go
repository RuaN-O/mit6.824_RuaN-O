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
	//	"bytes"

	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//2A
	State *state
	//2B
	applyCh chan ApplyMsg
}

type state struct {
	isleader         int
	CurrentTerm      int
	VoteFor          int
	Logs             []etlog
	heartchan        chan struct{}
	singleappendChan chan struct{}
	commitchan       chan struct{}

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

type etlog struct {
	Term    int
	Index   int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int

	Entries []etlog

	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictTerm  int
	ConflictIndex int
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.State.CurrentTerm
	isleader = rf.State.isleader == LEADER
	return term, isleader
}

// func (rf *Raft) persistGo() { //每300ms自动调用一次persist
// 	for {
// 		timeout := time.Duration(300 * time.Millisecond)
// 		select {
// 		case <-rf.State.persistchan:
// 			continue
// 		case <-time.After(timeout):
// 			rf.persistWithLock()
// 		}
// 	}
// }

// func (rf *Raft) Persist() {
// 	rf.persist()
// 	rf.State.persistchan <- struct{}{}
// }

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.

func (rf *Raft) persistWithLock() {
	rf.mu.Lock()
	rf.persist()
	rf.mu.Unlock()
}
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	//对于所有的follower都需要持久化的字段
	//这里不加锁
	//理由是需要插入在关键状态更改时，而关键状态更改时已经做过了加锁的操作

	e.Encode(rf.State.CurrentTerm)
	e.Encode(rf.State.VoteFor)
	e.Encode(rf.State.Logs)

	data := w.Bytes()

	rf.persister.SaveRaftState(data)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VoteFor int
	var Logs []etlog

	if d.Decode(&CurrentTerm) != nil || d.Decode(&VoteFor) != nil || d.Decode(&Logs) != nil {
		fmt.Println("Decode Error解码错误step1")
		return
	}

	rf.mu.Lock()
	rf.State.CurrentTerm = CurrentTerm
	rf.State.VoteFor = VoteFor
	rf.State.Logs = Logs
	rf.mu.Unlock()

}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!

// 发起一轮新的选举

func (rf *Raft) election() int {
	if rf.State.isleader == LEADER {
		return LEADER
	}

	// ok := rf.PingGo()
	// if !ok { //网络有问题
	// 	time.Sleep(10 * time.Millisecond)
	// 	return FOLLOWER
	// }

	rf.mu.Lock()
	self := rf.State
	self.isleader = CANDIDATER
	self.VoteFor = rf.me
	self.CurrentTerm++
	term := self.CurrentTerm
	lastlogposition := max(0, len(self.Logs)-1)
	lastlogidx := self.Logs[lastlogposition].Index
	lastlogterm := self.Logs[lastlogposition].Term

	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastlogidx,
		LastLogTerm:  lastlogterm,
	}

	rf.mu.Unlock()
	//votebuckct := make(chan struct{}, len(rf.peers))
	var votecounter int32
	atomic.AddInt32(&votecounter, 1)
	rf.electionhelper(&votecounter, &args)
	//计票前看看是否已经有leader产生

	//time.Sleep(5 * time.Millisecond)

	rf.mu.Lock()
	if self.isleader == FOLLOWER {
		rf.mu.Unlock()
		return FOLLOWER
	}

	if int(votecounter) > (len(rf.peers))/2 { //选举成功
		self.isleader = LEADER
		rf.mu.Unlock()
		//立即向所有node发送心跳
		go rf.leaderstuff()
		return LEADER
	} else {
		self.isleader = FOLLOWER
		rf.mu.Unlock()
		return FOLLOWER
	}
}

func (rf *Raft) electionhelper(votecounter *int32, args *RequestVoteArgs) int {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	targets := []int{}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		targets = append(targets, i)
	}

	var wg sync.WaitGroup

	for _, t := range targets {
		wg.Add(1)
		go func(ctx context.Context, t int) {
			defer wg.Done()
			ch := make(chan struct{}, 1)
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(t, args, &reply)
			if ok {
				if reply.Term > args.Term {
					rf.mu.Lock()
					rf.State.isleader = FOLLOWER
					rf.State.CurrentTerm = reply.Term
					rf.State.VoteFor = -1
					rf.persist()
					rf.mu.Unlock()
					return
				}
				if reply.VoteGranted {
					atomic.AddInt32(votecounter, 1)
				}
				ch <- struct{}{}
			}
			select {
			case <-ctx.Done():
				return
			case <-ch:
				return
			}
		}(ctx, t)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	//随机超时选举超时时间
	randtime := rand.Intn(100) + 150
	select {
	case <-done:
	case <-time.After(time.Duration(randtime) * time.Millisecond):
	}

	currentCount := atomic.LoadInt32(votecounter)
	// ...
	return int(currentCount)

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	//接到一个投票请求后
	//怎么处理投还是不投
	rf.mu.Lock()
	defer rf.mu.Unlock()

	state := rf.State
	reply.Term = state.CurrentTerm
	if args.Term < state.CurrentTerm || state.isleader == LEADER || state.isleader == CANDIDATER {
		reply.VoteGranted = false
		return
	}

	if args.Term > state.CurrentTerm {
		state.VoteFor = -1
		state.isleader = FOLLOWER
		state.CurrentTerm = args.Term
		reply.Term = state.CurrentTerm
	}

	//logs := state.logs
	// 检查日志是否至少一样新
	lastLog := state.Logs[len(state.Logs)-1]
	logOk := (args.LastLogTerm > lastLog.Term) ||
		(args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)

	canVote := (state.VoteFor == -1 || state.VoteFor == args.CandidateId) && logOk

	if canVote {
		reply.VoteGranted = true
		state.VoteFor = args.CandidateId
		// 重置选举超时
		select {
		case state.heartchan <- struct{}{}:
		default:
		}
	} else {
		reply.VoteGranted = false
	}
	rf.persist()

}

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

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.State.isleader != LEADER {
		return index, term, isLeader
	}

	lastLogIndex := rf.State.Logs[len(rf.State.Logs)-1].Index
	newlog := etlog{
		Term:    rf.State.CurrentTerm,
		Index:   lastLogIndex + 1,
		Command: command,
	}

	isLeader = true

	rf.State.Logs = append(rf.State.Logs, newlog)
	//fmt.Printf("leader:%v 已追加报文\n", rf.me)
	rf.State.nextIndex[rf.me] = newlog.Index + 1
	rf.State.matchIndex[rf.me] = newlog.Index
	//向大家广播同步这条日志
	go rf.singelAppendEntriesGo()
	rf.persist()

	return newlog.Index, newlog.Term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		electionTimeout := time.Duration(250+rand.Intn(150)) * time.Millisecond

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.State.heartchan:
			//rf.drainHeartBearChan()
			//time.Sleep(200 * time.Millisecond)
			continue
		// case <-rf.state.commitchan:
		// 	rf.applyEntrieshelper()

		case <-time.After(electionTimeout):
			//触发选举流程
			rf.election()

		}
	}
}

// leader向所有节点同步日志||心跳

func (rf *Raft) singelAppendEntriesGo() { //广播某条日志
	if rf.State.isleader != LEADER {
		return
	}
	targets := []int{}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		targets = append(targets, i)
	}
	//每一个target的信息不一样，需要分别发送
	rf.mu.Lock()
	self := rf.State

	for _, t := range targets {
		nextidx := self.nextIndex[t]
		prevlogidx := nextidx - 1

		prevlogTerm := self.Logs[prevlogidx].Term
		entries := make([]etlog, 0)
		entries = append(entries, self.Logs[prevlogidx+1:]...)

		//fmt.Printf("singleappendets:%v\n", entries)
		args := AppendEntriesArgs{
			Term:         self.CurrentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevlogidx,
			PrevLogTerm:  prevlogTerm,
			Entries:      entries,
			LeaderCommit: self.commitIndex,
		}
		reply := AppendEntriesReply{}
		go rf.appendEntriesToOne(t, &args, &reply)
	}
	rf.mu.Unlock()
	rf.State.singleappendChan <- struct{}{}
}

func (rf *Raft) AppendEntriesGo() { //正常的日志复制心跳，定期执行
	for {
		timeout := time.Duration(100 * time.Millisecond)
		select {
		case <-rf.State.singleappendChan:
			continue
		case <-time.After(timeout):
			rf.singelAppendEntriesGo()
		}
	}
}

func (rf *Raft) appendEntriesToOne(t int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.State.isleader != LEADER {
		return
	}
	ok := rf.sendAppendEntries(t, args, reply)
	//self.appendentriesreply <- reply
	if ok {
		go rf.handleAppendEntriesReply(t, args, reply)
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	self := rf.State

	//判断是否是同一轮心跳
	reply.Term = self.CurrentTerm
	reply.Success = false
	if args.Term < self.CurrentTerm {
		rf.mu.Unlock()
		return
	}

	// 收到合法心跳，重置选举超时
	select {
	case self.heartchan <- struct{}{}:
	default:
	}
	//新的任期到来，更新自身信息
	if args.Term > self.CurrentTerm {
		self.CurrentTerm = args.Term
		self.VoteFor = -1
		self.isleader = FOLLOWER
		reply.Term = self.CurrentTerm
		rf.persist()
	}

	var uptodate bool

	if args.PrevLogIndex >= len(self.Logs) {
		reply.Term = self.CurrentTerm
		//快速找到conflictTerm和conflictIdx
		reply.ConflictTerm = self.Logs[len(self.Logs)-1].Term
		idx := self.Logs[len(self.Logs)-1].Index
		for tidx := idx - 1; tidx >= 1; tidx-- {
			if self.Logs[tidx].Term == reply.ConflictTerm {
				idx = tidx
			} else {
				break
			}
		}
		reply.ConflictIndex = idx
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	if self.Logs[args.PrevLogIndex].Term == args.PrevLogTerm {
		uptodate = true
	} else {
		reply.ConflictTerm = self.Logs[args.PrevLogIndex].Term
		idx := self.Logs[args.PrevLogIndex].Index
		for tidx := idx - 1; tidx >= 1; tidx-- {
			if self.Logs[tidx].Term == reply.ConflictTerm {
				idx = tidx
			} else {
				break
			}
		}
		reply.ConflictIndex = idx
		//删除冲突点之后的日志
		self.Logs = self.Logs[:idx]
		rf.persist()
		reply.Success = false
	}

	if uptodate {

		//fmt.Printf("node:%v的commitIdx:%v\n", rf.me, rf.state.commitIndex)
		// 如果是心跳（Entries为空），直接返回成功
		if len(args.Entries) == 0 {
			self.commitIndex = max(args.LeaderCommit, rf.State.commitIndex)
			reply.Success = true
			rf.mu.Unlock()
			//rf.state.commitchan <- struct{}{}
			rf.applyEntrieshelper()
			return
		}
		//fmt.Printf("node:%v,收到报文:%v并处理\n", rf.me, args.Entries)
		//fmt.Printf("node:%v,报文处理前logs:%v\n", rf.me, rf.state.logs)

		newlogslen := len(self.Logs[:args.PrevLogIndex+1])
		//fmt.Println("newlogs长度:", newlogslen)
		newlogs := make([]etlog, newlogslen)
		//fmt.Printf("node:%v,newlogs初始化logs:%v\n", rf.me, newlogs)
		copy(newlogs[:args.PrevLogIndex+1], self.Logs[:args.PrevLogIndex+1])
		//fmt.Printf("node:%v,newlogs处理前logs:%v\n", rf.me, newlogs)
		newlogs = append(newlogs, args.Entries...)
		//fmt.Printf("node:%v,newlogs处理后logs:%v\n", rf.me, newlogs)
		self.Logs = newlogs
		self.matchIndex[rf.me] = args.PrevLogIndex + len(args.Entries)
		self.nextIndex[rf.me] = self.matchIndex[rf.me] + 1

		self.commitIndex = max(args.LeaderCommit, rf.State.commitIndex)
		rf.persist()

		rf.mu.Unlock()
		rf.applyEntrieshelper()
		//rf.state.commitchan <- struct{}{}

		//fmt.Printf("node:%v 已成功处理appendets success\n", rf.me)
		//fmt.Printf("node:%v 操作日志:%v\n", rf.me, opmap[rf.me])
		//fmt.Printf("node:%v,报文处理后logs:%v\n", rf.me, rf.state.logs)

		reply.Success = true
	}

}

func (rf *Raft) handleAppendEntriesReply(t int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.State.isleader != LEADER {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Success {

		rf.State.matchIndex[t] = args.PrevLogIndex + len(args.Entries)
		rf.State.nextIndex[t] = args.PrevLogIndex + len(args.Entries) + 1
		//fmt.Printf("node:%v's nextIndex:%v, matchIndex:%v\n", t, rf.state.nextIndex[t], rf.state.matchIndex[t])

		//推进提交
		if rf.State.matchIndex[t] > rf.State.commitIndex {
			go rf.pushCommit()
		}
		return
	} else {

		if reply.Term > rf.State.CurrentTerm { //降级为follower
			rf.State.isleader = FOLLOWER
			rf.State.VoteFor = -1
			rf.State.CurrentTerm = reply.Term
			rf.persist()

			return
		}

		//找到符合要求的日志idx,递减到1
		newargs := AppendEntriesArgs{
			Term:         args.Term,
			LeaderId:     args.LeaderId,
			PrevLogIndex: args.PrevLogIndex,
			PrevLogTerm:  args.PrevLogTerm,
			Entries:      nil,
			LeaderCommit: args.LeaderCommit,
		}

		newreply := AppendEntriesReply{}
		newargs.PrevLogIndex = max(reply.ConflictIndex-1, 0)

		newargs.PrevLogTerm = rf.State.Logs[newargs.PrevLogIndex].Term
		newargs.Entries = rf.State.Logs[newargs.PrevLogIndex+1:]

		go rf.appendEntriesToOne(t, &newargs, &newreply)

	}
}

func (rf *Raft) pushCommit() {
	//fmt.Println("推进提交idx:", idx)

	matches := make([]int, len(rf.peers))
	rf.mu.Lock()
	copy(matches, rf.State.matchIndex)
	rf.mu.Unlock()
	sort.Ints(matches)
	N := matches[len(matches)/2]
	//只能提交自己任期内的日志，避免论文中figure8 2C 的双leader同时提交的极端情况
	if N > rf.State.commitIndex && rf.State.Logs[N].Term == rf.State.CurrentTerm {
		rf.mu.Lock()
		rf.State.commitIndex = N
		rf.mu.Unlock()
		//fmt.Println("推进提交成功，commitidx:", rf.state.commitIndex)
		//rf.state.commitchan <- struct{}{}
		rf.applyEntrieshelper()
	}
}

func (rf *Raft) applyEntries() { //更新自己的lastapply和commitidx
	for {
		select {
		case <-rf.State.commitchan:
			rf.applyEntrieshelper()
		default:
		}
	}
}

func (rf *Raft) applyEntrieshelper() {
	if rf.State.lastApplied < rf.State.commitIndex {
		rf.mu.Lock()
		for rf.State.lastApplied < rf.State.commitIndex {
			var ok bool
			if rf.State.lastApplied+1 < len(rf.State.Logs) {
				ok = rf.doEntry(&rf.State.Logs[rf.State.lastApplied+1])
			}
			if ok {
				applymsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.State.Logs[rf.State.lastApplied+1].Command,
					CommandIndex: rf.State.Logs[rf.State.lastApplied+1].Index,
				}
				rf.State.lastApplied++
				rf.applyCh <- applymsg
				//fmt.Println(rf.me, "应用到状态机成功,lastapplied:", rf.state.lastApplied)
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) doEntry(log *etlog) bool {
	return true
}

func (rf *Raft) leaderstuff() { //领导负责的事务，心跳，处理心跳回复，以及todo appendentries
	//go rf.sendHeartBeat()
	rf.AppendEntriesGo()

	//go rf.solveHeartBeatChan() //包含了处理heartbeat和appendentriesreply

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).
	//2A
	state := state{
		isleader:    FOLLOWER,
		CurrentTerm: 0,
		VoteFor:     -1,

		commitIndex:      0,
		lastApplied:      0,
		Logs:             make([]etlog, 0),
		heartchan:        make(chan struct{}, 50),
		singleappendChan: make(chan struct{}, 50),
		commitchan:       make(chan struct{}, 100),
		nextIndex:        make([]int, len(rf.peers)),
		matchIndex:       make([]int, len(rf.peers)),
	}
	state.Logs = append(state.Logs, etlog{
		Term:    0,
		Index:   0,
		Command: nil,
	})
	for i := range state.nextIndex {
		state.nextIndex[i] = 1 // 从索引1开始
		state.matchIndex[i] = 0
	}
	rf.State = &state
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
