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

type LogEntry struct {
	Term int
	Data []byte
}

// A Go object implementing a single Raft peer.
type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	stateLock   sync.Mutex // 用于保护接下来三个变量
	currentTerm int
	state       RaftState
	voteFor     int
	log         []LogEntry

	// Follower
	lastContact atomic.Value // 上一次接受到 Leader 心跳信息的时间

	// Candidate
	grantedVotes uint32 // 本轮收到的票数
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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()

	var term = rf.currentTerm
	var isleader = (rf.state == Leader)
	return term, isleader
}

// Term 增加，同时更新为对应的状态
// 注：term == -1 表示 rf.currentTerm 自增 1，term == 0 表示 currentTerm 不变，
// 否则更新为对应的 term
func (rf *Raft) nextState(state RaftState, term int, holdLock bool) {
	if !holdLock {
		rf.stateLock.Lock()
		defer rf.stateLock.Unlock()
	}

	switch term {
	case -1:
		rf.currentTerm++
	case 0:
		// do nothing
	default:
		if rf.currentTerm != term {
			Debug(dTerm, "[S%v's Term: %v -> %v] S%v's term changed", rf.me, rf.currentTerm, term, rf.me)
		}
		rf.currentTerm = term
	}

	if state != rf.state {
		Debug(dState, "[S%v's State: %s -> %s] S%v's state changed", rf.me, rf.state, state, rf.me)
	}
	switch state {
	case Leader:
		rf.state = Leader
	case Follower:
		rf.state = Follower
		if term != 0 {
			rf.voteFor = -1
		}
	case Candidate:
		rf.state = Candidate
		rf.voteFor = rf.me
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
	Term        int
	CandidateId int
	// lastLogTerm  int
	// lastLogIndex int
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
	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()

	if args.Term < rf.currentTerm {
		Debug(dVote, "[S%v -> S%v]S%v receive a expired RequestVote", args.CandidateId, rf.me, rf.me)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 判断是否为新一轮新的 term
	if args.Term > rf.currentTerm {
		rf.nextState(Follower, args.Term, true)
	}

	// term 已经可以确认下来
	reply.Term = rf.currentTerm

	// 检查日志新旧
	// lastLogIndex := len(rf.log) - 1
	// lastLogTerm := rf.log[lastLogIndex].term
	// if args.lastLogTerm < lastLogTerm ||
	// 	args.lastLogTerm == lastLogTerm && args.lastLogIndex < lastLogIndex {
	// 	reply.voteGranted = false
	// 	return
	// }

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
	Term     int
	LeaderId int
	Entries  []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.stateLock.Lock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 说明存在一个 term 大于等于自己的 Leader，当前节点必须为 Follower
	rf.nextState(Follower, args.Term, true)
	rf.stateLock.Unlock()

	Debug(dAlive, "[S%v -> S%v] S%v receive heartbeat", args.LeaderId, rf.me, rf.me)
	rf.setLastContact(time.Now())
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
func (rf *Raft) quorumSize() int {
	// 所有节点数除以 2 向上取整
	return (len(rf.peers) + 1) / 2
}

func (rf *Raft) startElection(currentTerm int) {
	rf.setGrantedVotes(1) // 先给自己投一票

	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}

		server := server // 防止循环变量快照问题
		go func() {
			args := RequestVoteArgs{
				Term:        currentTerm,
				CandidateId: rf.me,
				// lastLogTerm:  lastLogTerm,
				// lastLogIndex: lastLogIndex,
			}
			reply := RequestVoteReply{}

			Debug(dVote, "[S%v -> S%v] S%v send a RequestVote", rf.me, server, rf.me)
			if !rf.sendRequestVote(server, &args, &reply) {
				return
			}

			rf.stateLock.Lock()
			if reply.Term < rf.currentTerm { // 说明选票过期了，当前节点开启的新的选举或者已经不是 Candidate
				rf.stateLock.Unlock()
				return
			}
			if reply.Term > rf.currentTerm { // 说明当前节点过期了
				rf.nextState(Follower, reply.Term, true)
				rf.stateLock.Unlock()
				return
			}
			rf.stateLock.Unlock()

			if reply.VoteGranted {
				Debug(dVote, "[S%v <- S%v] S%v receive a vote", rf.me, server, rf.me)
				rf.addGrantedVotes(1)
			}
		}()
	}
}

func (rf *Raft) runFollower(contactLeader bool) {
	rf.stateLock.Lock()
	// 如果时限内没有收到心跳信息
	if !contactLeader && rf.voteFor == -1 {
		Debug(dTimer, "S%v time out, start election", rf.me)

		rf.nextState(Candidate, -1, true)
		currentTerm := rf.currentTerm
		rf.stateLock.Unlock()
		rf.startElection(currentTerm)
		return
	}
	rf.stateLock.Unlock()
}

func (rf *Raft) runCandidate() {
	// 可能接受了其他节点 Term 更高的信息而转变为 Follower，因此这里需检测状态是否已经发生改变
	rf.stateLock.Lock()
	if rf.state != Candidate {
		rf.stateLock.Unlock()
		return
	}

	if int(rf.getGrantedVotes()) >= rf.quorumSize() { // 获得大半选票，当选 Leader
		Debug(dVote, "S%v receives votes from majority of servers", rf.me)
		rf.nextState(Leader, 0, true)
		rf.stateLock.Unlock()
		return
	}

	//发起一轮新的选举
	Debug(dTimer, "S%v time out, start a new election", rf.me)
	rf.nextState(Candidate, -1, true)
	currentTerm := rf.currentTerm
	rf.stateLock.Unlock()
	// lastLogIndex := len(rf.log)
	// lastLogTerm := rf.log[lastLogIndex].term

	rf.startElection(currentTerm)
}

func (rf *Raft) runLeader() {
	// 可能已经退位了
	rf.stateLock.Lock()
	if rf.state != Leader {
		rf.stateLock.Unlock()
		return
	}

	// 向所有 Follower 发送心跳信号
	Debug(dTimer, "S%v send heartbeat to all servers", rf.me)
	currentTerm := rf.currentTerm
	rf.stateLock.Unlock()

	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			continue
		}

		server := server // 防止循环变量快照问题
		go func() {
			args := AppendEntriesArgs{
				Term:     currentTerm,
				LeaderId: rf.me,
			}
			reply := AppendEntriesReply{}

			Debug(dAlive, "[S%v -> S%v] S%v send heartbeat", rf.me, server, rf.me)
			if !rf.SendAppendEntries(server, &args, &reply) {
				return
			}

			rf.stateLock.Lock()
			defer rf.stateLock.Unlock()
			if reply.Term > rf.currentTerm {
				rf.nextState(Follower, reply.Term, true)
			}
		}()
	}
}

// 执行特定状态对应的定时任务
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (2A)
		electionTimeout := time.Duration(50 + (rand.Int63() % 300))

		rf.stateLock.Lock()
		switch rf.state {
		case Follower:
			timeout := electionTimeout * time.Millisecond
			Debug(dTimer, "S%v reset timer with %v timeout", rf.me, timeout)
			rf.stateLock.Unlock()
			time.Sleep(timeout)

			rf.runFollower(time.Since(rf.getLastContact()) <= timeout)
		case Candidate:
			timeout := electionTimeout * time.Millisecond
			Debug(dTimer, "S%v reset timer with %v timeout", rf.me, timeout)
			rf.stateLock.Unlock()
			time.Sleep(timeout)

			rf.runCandidate()
		case Leader:
			timeout := electionTimeout * time.Millisecond / 5
			Debug(dTimer, "S%v reset timer with %v timeout", rf.me, timeout)
			rf.stateLock.Unlock()
			time.Sleep(timeout)

			rf.runLeader()
		}
	}
	Debug(dState, "[dead] S%v is killed", rf.me)
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
	rf.state = Follower
	rf.voteFor = -1
	rf.setLastContact(time.Time{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
