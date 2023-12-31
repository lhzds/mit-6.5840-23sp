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
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
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

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

func (s RaftState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

type FollowerRepl struct {
	// 用于唤醒协程
	shouldStop bool // 用于判断协程是否需要停止
	shouldRepl bool // 用于判断协程需要执行复制
	mu         sync.Mutex
	cond       sync.Cond

	server    int // 要复制的服务器
	nextIndex int // 下一个要发送的日志索引
}

type Commitment struct {
	startIndex  int     // 大于等于这个下标的日志项都是本 term 的，才能通过计数进行提交
	matchIndexs []int32 // 日志复制情况，基于这个计算可以更新的 commitIndex
}

func (commitment *Commitment) setMatchIndex(server int, matchIndex int) {
	atomic.StoreInt32(&commitment.matchIndexs[server], int32(matchIndex))
}

func (commitment *Commitment) getMatchIndex(server int) int {
	return int(atomic.LoadInt32(&commitment.matchIndexs[server]))
}

type LogEntry struct {
	Term int
	Cmd  interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	mu          sync.Mutex // 用于保护接下来三个变量
	CurrentTerm int
	State       RaftState
	VoteFor     int

	logMu sync.Mutex // 用于保护日志
	Log   []LogEntry // 日志

	commitIndex int32 // 当前节点已经提交的最大日志 Index
	lastApplied int   //当前已经应用到状态机的最大日志 Index

	// 用于提交日志
	shouldApply bool // 作为唤醒 apply 协程的判断条件
	applyCond   sync.Cond
	applyMu     sync.Mutex
	applyCh     chan ApplyMsg

	// Follower
	lastContact atomic.Value // 上一次接受到 Leader 心跳信息或者成功投票给 Candidate 的时间

	// Candidate
	grantedVotes uint32 // 本轮收到的票数

	// Leader
	repls      atomic.Value
	commitment *Commitment
}

func (rf *Raft) setRepls(repls []FollowerRepl) {
	rf.repls.Store(repls)
}

func (rf *Raft) getRepls() []FollowerRepl {
	return rf.repls.Load().([]FollowerRepl)
}

func (rf *Raft) getGrantedVotes() uint32 {
	return atomic.LoadUint32(&rf.grantedVotes)
}

func (rf *Raft) setGrantedVotes(grantedVotes uint32) {
	atomic.StoreUint32(&rf.grantedVotes, grantedVotes)
}

func (rf *Raft) addGrantedVotes(delta uint32) {
	atomic.AddUint32(&rf.grantedVotes, delta)
}

func (rf *Raft) getLastContact() time.Time {
	return rf.lastContact.Load().(time.Time)
}

func (rf *Raft) setLastContact(t time.Time) {
	rf.lastContact.Store(t)
}

func (rf *Raft) setCommitIndex(commitIndex int) {
	atomic.StoreInt32(&rf.commitIndex, int32(commitIndex))
}

func (rf *Raft) getCommitIndex() int {
	return int(atomic.LoadInt32(&rf.commitIndex))
}

func (rf *Raft) getRaftState() RaftState {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.State
}

func (rf *Raft) getLastIndex() int {
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	return len(rf.Log) - 1
}

func (rf *Raft) getLastIndexAndTerm() (lastIndex int, lastTerm int) {
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	lastIndex = len(rf.Log) - 1
	lastTerm = -1
	if lastIndex >= 0 {
		lastTerm = rf.Log[lastIndex].Term
	}
	return
}

func (rf *Raft) getLogTerm(index int) (term int) {
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	term = -1
	if index >= 0 {
		term = rf.Log[index].Term
	}
	return
}

func (rf *Raft) getLogEntries(start int, end int) []LogEntry {
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	return rf.Log[start:end]
}

func (rf *Raft) writeLogEntriesImpl(start int, entries ...LogEntry) {
	// 跳过重复的日志项
	newEntries := make([]LogEntry, 0)
	for i := 0; i < len(entries); i++ {
		if start >= len(rf.Log) || entries[i].Term != rf.Log[start].Term {
			newEntries = entries[i:]
			break
		}
		start++
	}
	if len(newEntries) == 0 {
		return
	}

	// 删除不一致的项
	if start < len(rf.Log) {
		Debug(dRepl, "[S%v's Len(Log): %v -> %v] S%v delete inconsistent entries",
			rf.me, len(rf.Log), start, rf.me)
		rf.Log = rf.Log[:start]
	}

	// 添加新的项
	Debug(dRepl, "[S%v's Len(Log): %v -> %v] S%v write entries locally",
		rf.me, start, start+len(newEntries), rf.me)
	rf.Log = append(rf.Log, newEntries...)
	rf.persist()
}

func (rf *Raft) writeLogEntries(start int, entries ...LogEntry) {
	if len(entries) == 0 {
		return
	}
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	rf.writeLogEntriesImpl(start, entries...)
}

func (rf *Raft) appendLogEntries(entries ...LogEntry) (appendIdx int) {
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	appendIdx = len(rf.Log)
	rf.writeLogEntriesImpl(len(rf.Log), entries...)
	return
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term = rf.CurrentTerm
	var isleader = (rf.State == Leader)
	return term, isleader
}

// 新选举产生的 Leader 需要执行的操作
func (rf *Raft) enThrone(currentTerm int) {
	rf.commitment = &Commitment{
		startIndex:  rf.getLastIndex() + 1,
		matchIndexs: make([]int32, len(rf.peers)),
	}
	for i := range rf.commitment.matchIndexs {
		rf.commitment.matchIndexs[i] = -1
	}

	// 向所有节点发送心跳信号
	rf.heartbeat(currentTerm)

	repls := make([]FollowerRepl, len(rf.peers))
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}

		repls[server].server = server
		repls[server].cond = *sync.NewCond(&repls[server].mu)
		repls[server].nextIndex = rf.getLastIndex() + 1 // 初始时先不开始复制

	}
	rf.setRepls(repls)

	// 启动复制协程
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}
		go rf.replicator(&repls[server], rf.commitment)
	}

}

// Leader 退位时的清理工作
func (rf *Raft) stepDown() {
	// 停止所有的复制协程
	repls := rf.getRepls()
	for i := 0; i < len(repls); i++ {
		repls[i].mu.Lock()
		repls[i].shouldStop = true
		repls[i].cond.Signal()
		repls[i].mu.Unlock()
	}
	// 设置数据结构为 nil，避免内存泄漏
	rf.commitment = nil
	rf.setRepls(nil)
}

// 更新 Term 以及状态，调用前需先持有锁
// (注：term == -1 表示 rf.currentTerm 自增 1，term == 0 表示 currentTerm 不变，
// 否则更新为对应的 term)
func (rf *Raft) nextTermAndState(newState RaftState, newTerm int) {
	// term 增加
	switch newTerm {
	case -1:
		Debug(dTerm, "[S%v's Term: %v -> %v] S%v's term changed", rf.me, rf.CurrentTerm,
			rf.CurrentTerm+1, rf.me)
		rf.CurrentTerm++
		rf.VoteFor = -1 // voteFor 记录本轮 Term 投给谁，因此 term 变了 voteFor 也要更新
	case 0:
		// do nothing
	default:
		if rf.CurrentTerm != newTerm {
			Debug(dTerm, "[S%v's Term: %v -> %v] S%v's term changed", rf.me, rf.CurrentTerm, newTerm, rf.me)
			rf.CurrentTerm = newTerm
			rf.VoteFor = -1
		}
	}

	if newTerm != 0 {
		rf.logMu.Lock()
		rf.persist()
		rf.logMu.Unlock()
	}

	if newState == rf.State {
		return
	}

	// 状态转变
	Debug(dState, "[S%v's State: %s -> %s] S%v's state changed", rf.me, rf.State, newState, rf.me)
	if rf.State == Leader {
		defer rf.stepDown() // 旧 Leader 退位
	}

	switch newState {
	case Leader:
		rf.State = Leader
		rf.enThrone(rf.CurrentTerm) // 新 Leader 上位
		go rf.leaderTicker()
	case Follower:
		rf.State = Follower
		go rf.followerTicker()
	case Candidate:
		rf.State = Candidate
		rf.VoteFor = rf.me
		go rf.candidateTicker()
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	buf := new(bytes.Buffer)
	e := labgob.NewEncoder(buf)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Log)
	data := buf.Bytes()
	rf.persister.Save(data, nil)
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
	buf := bytes.NewBuffer(data)
	d := labgob.NewDecoder(buf)
	var (
		CurrentTerm int
		VoteFor     int
		Log         []LogEntry
	)
	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VoteFor) != nil || d.Decode(&Log) != nil {
		return
	}

	rf.CurrentTerm = CurrentTerm
	rf.VoteFor = VoteFor
	rf.Log = Log
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
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogTerm  int
	LastLogIndex int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 判断 candidate 的 term 是否过期
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm {
		Debug(dVote, "[S%v -> S%v]S%v receive a expired RequestVote", args.CandidateId, rf.me, rf.me)
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	// 判断是否为新一轮新的 term
	if args.Term > rf.CurrentTerm {
		// 进入新 Term，voteFor才需要进行更新
		rf.nextTermAndState(Follower, args.Term)
	}

	// term 已经可以确认下来
	reply.Term = rf.CurrentTerm

	// 比较日志新旧
	lastLogIndex, lastLogTerm := rf.getLastIndexAndTerm()
	if args.LastLogTerm < lastLogTerm ||
		args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex {
		reply.VoteGranted = false
		return
	}

	// 如果本轮已经投过票，且票不是投给自己（先来先到原则）
	if rf.VoteFor != -1 && args.CandidateId != rf.VoteFor {
		Debug(dVote, "[S%v -> S%v] S%v refuses to vote due to having voted for other",
			args.CandidateId, rf.me, rf.me)
		reply.VoteGranted = false
		return
	}

	// 同意投票
	Debug(dVote, "[S%v -> S%v] S%v argees to vote", args.CandidateId, rf.me, rf.me)
	rf.VoteFor = args.CandidateId
	rf.setLastContact(time.Now())
	reply.VoteGranted = true
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	LeaderCommit int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// 用于快速重建
	XTerm  int
	XIndex int
	XLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	// 拒绝过期信息
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		rf.mu.Unlock()
		Debug(dAlive, "[S%v -> S%v] S%v receive expired rpc", args.LeaderId, rf.me, rf.me)
		return
	}

	// 说明存在一个 term 大于等于自己的 Leader，当前节点必须为 Follower
	if args.LeaderId != rf.me {
		rf.nextTermAndState(Follower, args.Term)
	}

	reply.Term = rf.CurrentTerm
	rf.mu.Unlock()

	rf.setLastContact(time.Now())

	// 如果是心跳信息
	if len(args.Entries) == 0 {
		goto SUCCESS
	}

	// 特判第0条日志项
	if args.PrevLogIndex == -1 {
		goto SUCCESS
	}

	// 开始一致性检测
	// 日志过短，不存在冲突的 Term
	if lastIndex := rf.getLastIndex(); lastIndex < args.PrevLogIndex {
		reply.XTerm = -1
		reply.XLen = args.PrevLogIndex - lastIndex
		goto FAIL
	}

	// term 冲突
	if xTerm := rf.getLogTerm(args.PrevLogIndex); xTerm != args.PrevLogTerm {
		// 过高的 Term 跳过，等同于日志过短的情况
		xIndex := args.PrevLogIndex
		if xTerm > args.PrevLogTerm {
			for xIndex >= 0 && rf.getLogTerm(xIndex) > args.PrevLogTerm {
				xIndex--
			}
			reply.XTerm = -1
			reply.XLen = args.PrevLogIndex - xIndex
			goto FAIL
		}

		// 当第一个 term 为 XTerm 日志项
		reply.XTerm = xTerm
		for xIndex-1 >= 0 && rf.getLogTerm(xIndex-1) == xTerm {
			xIndex--
		}
		reply.XIndex = xIndex
		goto FAIL
	}

SUCCESS:
	reply.Success = true

	// 日志项为空，说明为心跳消息
	if len(args.Entries) == 0 {
		Debug(dAlive, "[S%v -> S%v] S%v receive leader's heartbeat", args.LeaderId, rf.me, rf.me)
	} else {
		Debug(dRepl, "[S%v -> S%v] S%v prepare to write new entries",
			args.LeaderId, rf.me, rf.me)
		rf.mu.Lock()
		rf.writeLogEntries(args.PrevLogIndex+1, args.Entries...)
		rf.mu.Unlock()
	}

	// 进行提交
	if old, new := rf.getCommitIndex(), min(args.LeaderCommit, rf.getLastIndex()); new > old {
		rf.setCommitIndex(new)
		Debug(dCommit, "[S%v's CommitIndex: %v -> %v] S%v commit new entries",
			rf.me, old, new, rf.me)
		rf.notifyApply()
	}

	return

FAIL:
	reply.Success = false
	if len(args.Entries) == 0 {
		Debug(dAlive, "[S%v -> S%v] S%v receive leader's heartbeat", args.LeaderId, rf.me, rf.me)
		return
	}
	Debug(dRepl, "[S%v -> S%v] S%v refuses due to inconsistent prev entry",
		args.LeaderId, rf.me, rf.me)
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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
	// Your code here (2B).
	rf.mu.Lock()
	term := rf.CurrentTerm
	isLeader := (rf.State == Leader)
	var index int
	if isLeader { // 这部分用锁保护，避免退位导致 rf.commitment 变为 nil
		Debug(dRepl, "S%v accept a new cmd from client", rf.me)
		index = rf.appendLogEntries(LogEntry{Term: term, Cmd: command})
		rf.commitment.match(rf.me, rf.getLastIndex())
		rf.notifyRepl()
	} else {
		index = rf.getLastIndex() + 1
	}
	rf.mu.Unlock()

	return index + 1, term, isLeader // 上层应用下标从 1 开始，因此要比实际加一
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

// 返回大多数节点的数目
func quorumSize(n int) int {
	return (n + 1) / 2
}

func (rf *Raft) quorumSize() int {
	// 所有节点数除以 2 向上取整
	return quorumSize(len(rf.peers))
}

func (rf *Raft) startElection(currentTerm int) {
	rf.setGrantedVotes(1) // 先给自己投一票
	lastLogIndex, lastLogTerm := rf.getLastIndexAndTerm()

	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}

		server := server // 防止循环变量快照问题
		go func() {
			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  rf.me,
				LastLogTerm:  lastLogTerm,
				LastLogIndex: lastLogIndex,
			}
			reply := RequestVoteReply{}

			Debug(dVote, "[S%v -> S%v] S%v send a RequestVote", rf.me, server, rf.me)
			if !rf.sendRequestVote(server, &args, &reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term < rf.CurrentTerm { // 说明选票过期了，当前节点开启的新的选举或者已经不是 Candidate
				return
			}
			if reply.Term > rf.CurrentTerm { // 说明当前节点过期了
				rf.nextTermAndState(Follower, reply.Term)
				return
			}

			Debug(dVote, "[S%v <- S%v] S%v receive a vote", rf.me, server, rf.me)
			if rf.State == Candidate && reply.VoteGranted {
				rf.addGrantedVotes(1)
				if int(rf.getGrantedVotes()) >= rf.quorumSize() { // 获得大半选票，当选 Leader
					Debug(dVote, "S%v receives votes from majority of servers", rf.me)
					rf.nextTermAndState(Leader, 0)
					return
				}
			}
		}()
	}
}

func (rf *Raft) followerTicker() {
	for !rf.killed() && rf.getRaftState() == Follower {
		timeout := time.Duration(50+(rand.Int63()%300)) * time.Millisecond
		Debug(dTimer, "S%v reset timer with %v timeout", rf.me, timeout)
		time.Sleep(timeout)

		rf.mu.Lock()
		if rf.State != Follower {
			rf.mu.Unlock()
			break
		}

		// 如果本次计时内没有收到心跳或者投出过票
		if elapsed := time.Since(rf.getLastContact()); elapsed > timeout {
			Debug(dTimer, "S%v time out(%v pass since the last cantact), start election",
				rf.me, elapsed)

			rf.nextTermAndState(Candidate, -1)
			currentTerm := rf.CurrentTerm
			rf.mu.Unlock()
			rf.startElection(currentTerm)
			return
		}

		rf.mu.Unlock()
	}
}

func (rf *Raft) candidateTicker() {
	for !rf.killed() && rf.getRaftState() == Candidate {
		timeout := time.Duration(50+(rand.Int63()%300)) * time.Millisecond
		Debug(dTimer, "S%v reset timer with %v timeout", rf.me, timeout)
		time.Sleep(timeout)

		rf.mu.Lock()
		if rf.State != Candidate {
			rf.mu.Unlock()
			break
		}

		//发起一轮新的选举
		Debug(dTimer, "S%v time out, start a new election", rf.me)
		rf.nextTermAndState(Candidate, -1)
		currentTerm := rf.CurrentTerm
		rf.mu.Unlock()

		rf.startElection(currentTerm)
	}
}

func (rf *Raft) heartbeat(currentTerm int) {
	leaderCommit := rf.getCommitIndex()
	commitment := rf.commitment

	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}

		server := server // 防止循环变量快照问题
		go func() {
			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: min(leaderCommit, commitment.getMatchIndex(server)),
			}
			reply := AppendEntriesReply{}

			Debug(dAlive, "[S%v -> S%v] S%v send heartbeat", rf.me, server, rf.me)
			if !rf.SendAppendEntries(server, &args, &reply) {
				return
			}

			Debug(dAlive, "[S%v <- S%v] S%v receive a follower reponse", rf.me, server, rf.me)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.CurrentTerm {
				rf.nextTermAndState(Follower, reply.Term)
			}
		}()
	}
}

func (rf *Raft) leaderTicker() {
	for !rf.killed() && rf.getRaftState() == Leader {
		timeout := time.Duration(50+(rand.Int63()%300)) * time.Millisecond / 5
		Debug(dTimer, "S%v reset timer with %v timeout", rf.me, timeout)
		time.Sleep(timeout)

		rf.mu.Lock()
		if rf.State != Leader {
			rf.mu.Unlock()
			break
		}
		currentTerm := rf.CurrentTerm
		rf.mu.Unlock()

		// 向所有 Follower 发送心跳信号
		Debug(dAlive, "S%v send heartbeat to all servers", rf.me)
		rf.heartbeat(currentTerm)
	}
}

func (rf *Raft) notifyRepl() {
	// 唤醒复制协程进行日志分发
	repls := rf.getRepls()
	if repls == nil {
		return
	}
	for server := 0; server < len(repls); server++ {
		if server == rf.me {
			continue
		}

		repls[server].mu.Lock()
		if !repls[server].shouldRepl {
			repls[server].shouldRepl = true
			repls[server].cond.Signal()
		}
		repls[server].mu.Unlock()
	}
}

func (commitment *Commitment) match(server int, matchIndex int) int {
	commitment.setMatchIndex(server, matchIndex)

	// 排序取中位数，计算最大可以提交的 Index
	matchIndexs := make([]int, 0, len(commitment.matchIndexs))
	for i := 0; i < len(commitment.matchIndexs); i++ {
		matchIndexs = append(matchIndexs, commitment.getMatchIndex(i))
	}

	sort.Slice(matchIndexs, func(i, j int) bool {
		return matchIndexs[i] > matchIndexs[j]
	})

	// new >= rf.commitment.startIndex 确保是当前 term 才能进行提交
	if commitIndex := matchIndexs[quorumSize(len(matchIndexs))-1]; commitIndex >= commitment.startIndex {
		return commitIndex
	}
	return -1
}

func (rf *Raft) replOneRound(replState *FollowerRepl, commitment *Commitment) {
	lastIndex := rf.getLastIndex()
	rf.mu.Lock()
	currentTerm := rf.CurrentTerm
	rf.mu.Unlock()
	prevSuccess := false
	for !rf.killed() && replState.nextIndex <= lastIndex {
		replState.mu.Lock()
		if replState.shouldStop {
			replState.mu.Unlock()
			break
		}
		replState.mu.Unlock()

		prevLogIndex := replState.nextIndex - 1
		prevLogTerm := -1
		if replState.nextIndex > 0 {
			prevLogTerm = rf.getLogTerm(replState.nextIndex - 1)
		}

		args := AppendEntriesArgs{
			Term:         currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.getCommitIndex(),

			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
		}
		if prevSuccess {
			args.Entries = rf.getLogEntries(replState.nextIndex, lastIndex+1)
		} else {
			args.Entries = rf.getLogEntries(replState.nextIndex, replState.nextIndex+1)
		}

		reply := AppendEntriesReply{}

		// 失败 nextIndex 没有更新，下一轮重新尝试
		Debug(dRepl, "[S%v -> S%v] S%v send a repl request(prevIndex: %v, prevTerm: %v)",
			rf.me, replState.server, rf.me, prevLogIndex, prevLogTerm)
		if !rf.SendAppendEntries(replState.server, &args, &reply) {
			continue
		}

		// 如果过期则直接结束复制
		rf.mu.Lock()
		if rf.State != Leader || rf.CurrentTerm < reply.Term {
			rf.nextTermAndState(Follower, reply.Term)
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		prevSuccess = reply.Success
		// 复制成功，尝试进行提交
		if reply.Success {
			Debug(dRepl,
				"[S%v <- S%v] S%v receive a agreed repl response",
				rf.me, replState.server, rf.me)
			new := commitment.match(replState.server, replState.nextIndex+len(args.Entries)-1)
			old := rf.getCommitIndex()
			if new > old {
				Debug(dCommit, "[S%v's CommitIndex: %v -> %v] S%v commit new entries",
					rf.me, old, new, rf.me)
				rf.setCommitIndex(new)
				rf.notifyApply()
			}

			replState.nextIndex += len(args.Entries)
			continue
		}

		Debug(dRepl,
			"[S%v <- S%v] S%v receive a refuesd repl response(xTerm=%v, xIndex=%v, xLen=%v)",
			rf.me, replState.server, rf.me, reply.XTerm, reply.XIndex, reply.XLen)

		// follower 日志过短，不存在冲突项
		if reply.XTerm == -1 {
			replState.nextIndex -= reply.XLen
			continue
		}

		// 基于 Follower 冲突的那段 Term 进行快速重建
		tmpIndex := prevLogIndex // 标记最后一个可能相同的日志项
		for ; tmpIndex >= reply.XIndex; tmpIndex-- {
			tmpTerm := rf.getLogTerm(tmpIndex)
			if tmpTerm == reply.XTerm {
				break
			}
			if tmpTerm < reply.XTerm {
				tmpIndex = reply.XIndex - 1
				break
			}
		}
		if tmpIndex < reply.XIndex {
			for ; tmpIndex >= 0 && rf.getLogTerm(tmpIndex) >= reply.XTerm; tmpIndex-- {
			}
		}
		replState.nextIndex = tmpIndex + 1
	}
}

func (rf *Raft) replicator(replState *FollowerRepl, commitment *Commitment) {
	for !rf.killed() {
		// 唤醒复制协程
		replState.mu.Lock()
		for !replState.shouldRepl && !replState.shouldStop {
			replState.cond.Wait()
		}
		if replState.shouldStop {
			replState.mu.Unlock()
			return
		}

		// 执行一轮复制
		replState.shouldRepl = false
		replState.mu.Unlock()
		rf.replOneRound(replState, commitment)
	}
}

func (rf *Raft) notifyApply() {
	rf.applyMu.Lock()
	if !rf.shouldApply {
		rf.shouldApply = true
		rf.applyCond.Signal()
	}
	rf.applyMu.Unlock()
}

func (rf *Raft) applyOneRound() {
	for !rf.killed() && rf.lastApplied < rf.getCommitIndex() {
		entry := rf.getLogEntries(rf.lastApplied+1, rf.lastApplied+2)[0]
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied + 2, // 上层应用下标从 1 开始，因此要比实际加一
			Command:      entry.Cmd,
		}
		Debug(dCommit, "S%v try to apply log[%v](term: %v)", rf.me, rf.lastApplied,
			entry.Term)
		rf.applyCh <- applyMsg
		rf.lastApplied++
	}
}

// 用于将 committed 但未 applied 的命令 apply
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.applyMu.Lock()
		for !rf.shouldApply {
			rf.applyCond.Wait()
		}
		rf.shouldApply = false
		rf.applyMu.Unlock()
		rf.applyOneRound()
	}
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

	// Your initialization code here (2A, 2B, 2C).
	rf.State = Follower
	rf.VoteFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.applyCh = applyCh
	rf.applyCond = *sync.NewCond(&rf.applyMu)
	rf.setLastContact(time.Time{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 启动 follower 计时协程
	go rf.followerTicker()

	// 启动 apply 协程，用于对数据进行 apply
	go rf.applier()

	return rf
}
