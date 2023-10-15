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

	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	startIndex int // 大于等于这个下标的日志项都是本 term 的，才能通过计数进行提交

	mu          sync.Mutex
	matchIndexs []int // 日志复制情况，基于这个计算可以更新的 commitIndex
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
	currentTerm int
	state       RaftState
	voteFor     int

	logMu sync.Mutex // 用于保护日志
	log   []LogEntry // 日志

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
	repls      []FollowerRepl
	commitment *Commitment
}

func (rf *Raft) GetGrantedVotes() uint32 {
	return atomic.LoadUint32(&rf.grantedVotes)
}

func (rf *Raft) SetGrantedVotes(grantedVotes uint32) {
	atomic.StoreUint32(&rf.grantedVotes, grantedVotes)
}

func (rf *Raft) AddGrantedVotes(delta uint32) {
	atomic.AddUint32(&rf.grantedVotes, delta)
}

func (rf *Raft) GetLastContact() time.Time {
	return rf.lastContact.Load().(time.Time)
}

func (rf *Raft) SetLastContact(t time.Time) {
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
	return rf.state
}

func (rf *Raft) getLastIndex() int {
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	return len(rf.log) - 1
}

func (rf *Raft) getLastIndexAndTerm() (lastIndex int, lastTerm int) {
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	lastIndex = len(rf.log) - 1
	lastTerm = -1
	if lastIndex >= 0 {
		lastTerm = rf.log[lastIndex].Term
	}
	return
}

func (rf *Raft) getLogTerm(index int) (term int) {
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	term = -1
	if index >= 0 {
		term = rf.log[index].Term
	}
	return
}

func (rf *Raft) getLogEntries(start int, end int) []LogEntry {
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	return rf.log[start:end]
}

func (rf *Raft) writeLogEntriesImpl(start int, entries ...LogEntry) {
	if start < len(rf.log) {
		Debug(dRepl, "[S%v's Len(Log): %v -> %v] S%v delete inconsistent entries",
			rf.me, len(rf.log), start, rf.me)
		rf.log = rf.log[:start]
	}

	Debug(dRepl, "[S%v's Len(Log): %v -> %v] S%v write entries locally",
		rf.me, start, start+len(entries), rf.me)
	rf.log = append(rf.log, entries...)
}

func (rf *Raft) writeLogEntries(start int, entries ...LogEntry) {
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	rf.writeLogEntriesImpl(start, entries...)
}

func (rf *Raft) appendLogEntries(entries ...LogEntry) {
	rf.logMu.Lock()
	defer rf.logMu.Unlock()
	rf.writeLogEntriesImpl(len(rf.log), entries...)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term = rf.currentTerm
	var isleader = (rf.state == Leader)
	return term, isleader
}

// 新选举产生的 Leader 需要执行的操作
func (rf *Raft) enThrone(currentTerm int) {
	// 向所有节点发送心跳信号
	rf.heartbeat(currentTerm)

	// 初始化数据结构，以及启动复制协程
	rf.commitment = &Commitment{
		startIndex:  rf.getLastIndex() + 1,
		matchIndexs: make([]int, len(rf.peers)),
	}
	for i := range rf.commitment.matchIndexs {
		rf.commitment.matchIndexs[i] = -1
	}

	rf.repls = make([]FollowerRepl, len(rf.peers))
	for server := 0; server < len(rf.repls); server++ {
		if server == rf.me {
			continue
		}

		rf.repls[server].server = server
		rf.repls[server].cond = *sync.NewCond(&rf.repls[server].mu)
		rf.repls[server].nextIndex = rf.getLastIndex() + 1 // 初始时先不开始复制
		go rf.replicator(&rf.repls[server], rf.commitment)
	}

}

// Leader 退位时的清理工作
func (rf *Raft) stepDown() {
	// 停止所有的复制协程
	for i := 0; i < len(rf.repls); i++ {
		rf.repls[i].mu.Lock()
		rf.repls[i].shouldStop = true
		rf.repls[i].cond.Signal()
		rf.repls[i].mu.Unlock()
	}
	// 设置数据结构为 nil，避免内存泄漏
	rf.commitment = nil
	rf.repls = nil
}

// 更新 Term 以及状态
// (注：term == -1 表示 rf.currentTerm 自增 1，term == 0 表示 currentTerm 不变，
// 否则更新为对应的 term)
func (rf *Raft) nextTermAndState(newState RaftState, newTerm int, holdLock bool) {
	if !holdLock {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}

	// term 增加
	switch newTerm {
	case -1:
		Debug(dTerm, "[S%v's Term: %v -> %v] S%v's term changed", rf.me, rf.currentTerm,
			rf.currentTerm+1, rf.me)
		rf.currentTerm++
		rf.voteFor = -1 // voteFor 记录本轮 Term 投给谁，因此 term 变了 voteFor 也要更新
	case 0:
		// do nothing
	default:
		if rf.currentTerm != newTerm {
			Debug(dTerm, "[S%v's Term: %v -> %v] S%v's term changed", rf.me, rf.currentTerm, newTerm, rf.me)
		}
		rf.currentTerm = newTerm
		rf.voteFor = -1
	}

	if newState == rf.state {
		return
	}

	// 状态转变
	Debug(dState, "[S%v's State: %s -> %s] S%v's state changed", rf.me, rf.state, newState, rf.me)
	if rf.state == Leader {
		defer rf.stepDown() // 旧 Leader 退位
	}

	switch newState {
	case Leader:
		rf.state = Leader
		go rf.leaderTicker()
		defer rf.enThrone(rf.currentTerm) // 新 Leader 上位
	case Follower:
		rf.state = Follower
		go rf.followerTicker()
	case Candidate:
		rf.state = Candidate
		rf.voteFor = rf.me
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

	if args.Term < rf.currentTerm {
		Debug(dVote, "[S%v -> S%v]S%v receive a expired RequestVote", args.CandidateId, rf.me, rf.me)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 判断是否为新一轮新的 term
	if args.Term > rf.currentTerm {
		rf.nextTermAndState(Follower, args.Term, true)
	}

	// term 已经可以确认下来
	reply.Term = rf.currentTerm

	// 比较日志新旧
	lastLogIndex, lastLogTerm := rf.getLastIndexAndTerm()
	if args.LastLogTerm < lastLogTerm ||
		args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex {
		reply.VoteGranted = false
		return
	}

	// 如果本轮已经投过票，且票不是投给自己（先来先到原则）
	if rf.voteFor != -1 && args.CandidateId != rf.voteFor {
		Debug(dVote, "[S%v -> S%v] S%v refuses to vote due to having voted for other",
			args.CandidateId, rf.me, rf.me)
		reply.VoteGranted = false
		return
	}

	// 同意投票
	Debug(dVote, "[S%v -> S%v] S%v argees to vote", args.CandidateId, rf.me, rf.me)
	rf.voteFor = args.CandidateId
	rf.SetLastContact(time.Now())
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
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		Debug(dAlive, "[S%v -> S%v] S%v receive expired rpc", args.LeaderId, rf.me, rf.me)
		return
	}

	// 说明存在一个 term 大于等于自己的 Leader，当前节点必须为 Follower
	if args.LeaderId != rf.me {
		rf.nextTermAndState(Follower, args.Term, true)
	}

	reply.Term = rf.currentTerm
	rf.mu.Unlock()

	rf.SetLastContact(time.Now())

	// 开始一致性检测
	tmpIndex := args.PrevLogIndex

	// 特判第0条日志项
	if tmpIndex == -1 {
		goto SUCCESS
	}

	// 日志过短，不存在冲突的 Term
	if lastIndex := rf.getLastIndex(); lastIndex < tmpIndex {
		reply.XTerm = -1
		reply.XLen = args.PrevLogIndex - lastIndex
		goto FAIL
	}

	// term 冲突
	if rf.getLogTerm(tmpIndex) != args.PrevLogTerm {
		// 跳过过高的 Term（这一步可以不要）
		for tmpIndex >= 0 && rf.getLogTerm(tmpIndex) > args.PrevLogTerm {
			tmpIndex--
		}
		if tmpIndex < 0 { // 全部都大于，处理等同于日志过短不存在冲突项目的 Term 的情况
			reply.XTerm = -1
			reply.XLen = args.PrevLogIndex
			goto FAIL
		}

		// 当第一个 term 为 XTerm 日志项的 index
		for tmpIndex >= 0 && rf.getLogTerm(tmpIndex) == args.PrevLogTerm {
			tmpIndex--
		}

		reply.XIndex = tmpIndex + 1
		reply.Term = rf.getLogTerm(tmpIndex)
		goto FAIL
	}

SUCCESS:
	reply.Success = true
	// 进行提交
	if old, new := rf.getCommitIndex(), min(args.LeaderCommit, rf.getLastIndex()); new > old {
		rf.setCommitIndex(new)
		Debug(dCommit, "[S%v's CommitIndex: %v -> %v] S%v commit new entries",
			rf.me, old, new, rf.me)
		rf.notifyApply()
	}

	// 日志项为空，说明为心跳消息
	if len(args.Entries) == 0 {
		Debug(dAlive, "[S%v -> S%v] S%v receive leader's heartbeat", args.LeaderId, rf.me, rf.me)
		return
	}

	// 添加日志项
	Debug(dRepl, "[S%v -> S%v] S%v accept new entries",
		args.LeaderId, rf.me, rf.me)
	rf.writeLogEntries(tmpIndex+1, args.Entries...)
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
	index := rf.getLastIndex() + 2 // 上层应用下标从 1 开始，因此要比实际加一
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := (rf.state == Leader)
	rf.mu.Unlock()

	if isLeader {
		Debug(dRepl, "S%v accept a new cmd from client", rf.me)
		rf.appendLogEntries(LogEntry{Term: term, Cmd: command})
		rf.commitment.match(rf.me, rf.getLastIndex())
		rf.notifyRepl()
	}

	return index, term, isLeader
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
	rf.SetGrantedVotes(1) // 先给自己投一票
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
			if reply.Term < rf.currentTerm { // 说明选票过期了，当前节点开启的新的选举或者已经不是 Candidate
				return
			}
			if reply.Term > rf.currentTerm { // 说明当前节点过期了
				rf.nextTermAndState(Follower, reply.Term, true)
				return
			}

			Debug(dVote, "[S%v <- S%v] S%v receive a vote", rf.me, server, rf.me)
			if rf.state == Candidate && reply.VoteGranted {
				rf.AddGrantedVotes(1)
				if int(rf.GetGrantedVotes()) >= rf.quorumSize() { // 获得大半选票，当选 Leader
					Debug(dVote, "S%v receives votes from majority of servers", rf.me)
					rf.nextTermAndState(Leader, 0, true)
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
		if rf.state != Follower {
			rf.mu.Unlock()
			break
		}

		// 如果本次计时内没有收到心跳或者投出过票
		if elapsed := time.Since(rf.GetLastContact()); elapsed > timeout {
			Debug(dTimer, "S%v time out(%v pass since the last cantact), start election",
				rf.me, elapsed)

			rf.nextTermAndState(Candidate, -1, true)
			currentTerm := rf.currentTerm
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
		if rf.state != Candidate {
			rf.mu.Unlock()
			break
		}

		//发起一轮新的选举
		Debug(dTimer, "S%v time out, start a new election", rf.me)
		rf.nextTermAndState(Candidate, -1, true)
		currentTerm := rf.currentTerm
		rf.mu.Unlock()

		rf.startElection(currentTerm)
	}
}

func (rf *Raft) heartbeat(currentTerm int) {
	leaderCommit := rf.getCommitIndex()
	leaderCommitTerm := rf.getLogTerm(leaderCommit)
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}

		server := server // 防止循环变量快照问题
		go func() {
			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: leaderCommit,
				PrevLogIndex: leaderCommit,
				PrevLogTerm:  leaderCommitTerm,
			}
			reply := AppendEntriesReply{}

			Debug(dAlive, "[S%v -> S%v] S%v send heartbeat", rf.me, server, rf.me)
			if !rf.SendAppendEntries(server, &args, &reply) {
				return
			}

			Debug(dAlive, "[S%v <- S%v] S%v receive a follower reponse", rf.me, server, rf.me)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.nextTermAndState(Follower, reply.Term, true)
			}
		}()
	}
}

func (rf *Raft) leaderTicker() {
	for !rf.killed() && rf.getRaftState() == Leader {
		timeout := time.Duration(50+(rand.Int63()%300)) * time.Millisecond / 10
		Debug(dTimer, "S%v reset timer with %v timeout", rf.me, timeout)
		time.Sleep(timeout)

		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			break
		}
		currentTerm := rf.currentTerm
		rf.mu.Unlock()

		// 向所有 Follower 发送心跳信号
		Debug(dTimer, "S%v send heartbeat to all servers", rf.me)
		rf.heartbeat(currentTerm)
	}
}

func (rf *Raft) notifyRepl() {
	// 唤醒复制协程进行日志分发
	for server := 0; server < len(rf.repls); server++ {
		if server == rf.me {
			continue
		}

		rf.repls[server].mu.Lock()
		if !rf.repls[server].shouldRepl {
			rf.repls[server].shouldRepl = true
			rf.repls[server].cond.Signal()
		}
		rf.repls[server].mu.Unlock()
	}
}

func (commitment *Commitment) match(server int, matchIndex int) int {
	commitment.mu.Lock()
	defer commitment.mu.Unlock()
	commitment.matchIndexs[server] = matchIndex

	// 排序取中位数，计算最大可以提交的 Index
	matchIndexs := make([]int, 0, len(commitment.matchIndexs))
	for i := 0; i < len(commitment.matchIndexs); i++ {
		matchIndexs = append(matchIndexs, commitment.matchIndexs[i])
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
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	for replState.nextIndex <= lastIndex {
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
			Entries:      rf.getLogEntries(replState.nextIndex, replState.nextIndex+1),
		}
		reply := AppendEntriesReply{}

		// 失败 nextIndex 没有更新，下一轮重新尝试
		Debug(dRepl, "[S%v -> S%v] S%v send a repl request", rf.me, replState.server, rf.me)
		if !rf.SendAppendEntries(replState.server, &args, &reply) {
			continue
		}
		Debug(dRepl, "[S%v <- S%v] S%v receive a repl response", rf.me, replState.server, rf.me)

		// 如果过期则直接结束复制
		rf.mu.Lock()
		if rf.currentTerm < reply.Term {
			rf.nextTermAndState(Follower, reply.Term, true)
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		// 复制成功，尝试进行提交
		if reply.Success {
			new := commitment.match(replState.server, replState.nextIndex)
			old := rf.getCommitIndex()
			if new > old {
				Debug(dCommit, "[S%v's CommitIndex: %v -> %v] S%v commit new entries",
					rf.me, old, new, rf.me)
				rf.setCommitIndex(new)
				rf.notifyApply()
			}

			replState.nextIndex++
			continue
		}

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
	for rf.lastApplied < rf.getCommitIndex() {
		command := rf.getLogEntries(rf.lastApplied+1, rf.lastApplied+2)[0].Cmd
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied + 2, // 上层应用下标从 1 开始，因此要比实际加一
			Command:      command,
		}
		rf.applyCh <- applyMsg
		rf.lastApplied++
		Debug(dCommit, "S%v try to apply log[%v]", rf.me, rf.lastApplied)
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
	rf.applyCh = applyCh
	rf.applyCond = *sync.NewCond(&rf.applyMu)

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.voteFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.SetLastContact(time.Time{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 启动 follower 计时协程
	go rf.followerTicker()

	// 启动 apply 协程，用于对数据进行 apply
	go rf.applier()

	return rf
}
