package raft

//todo ...
//recover logs,  leader kickout, append log 长度
//append retry 机制
// write command to applt chan,then maintain a apply index and a command index

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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2

	ELECTION_TIMEOUT = 800 * time.Millisecond
	//LEADER_TIMEOUT = 1000 * time.Millisecond
	RANDOM_PLUS    = 200 * time.Millisecond
	HEART_INTERVAL = 300 * time.Millisecond
)

func AssertTrue(test bool, format string, a ...interface{}) {
	if !test {
		panic(fmt.Sprintf("S%d "+format, a...))
	}
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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
type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	//init need not persisit
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	major     int
	peerCnt   int

	//protected by atomic
	dead    int32 // set by Kill()
	applyCh chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	snapshot      []byte
	applyCond     *sync.Cond
	clientReqCond *sync.Cond
	timerLock     sync.Mutex
	//volatile
	role        int
	commitIndex int
	lastApplied int
	timeDdl     time.Time //for follower and candidate
	votes       int
	//volatile for leader,every election re-init
	nextIndex  []int
	matchIndex []int

	//persist !!!!!!!!!!!!
	currentTerm int //2a
	votedFor    int //2a
	logs        []LogEntry
	//persist, recover from crash, modified by some functions
	offset       int //known as snapLastIncludeIndex + 1
	lastLogIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == LEADER
	rf.mu.Unlock()
	return term, isleader
}

//hold lock
func (rf *Raft) doPersistRaftAndSnap(index, term int, snapshot []byte) {
	Logger(dSnap, "S%v term %d apply snapshot offset %d->%d \n",
		rf.me, rf.currentTerm, rf.offset, index)

	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	length_after_trim := rf.lastLogIndex - index + 1
	if length_after_trim < 1 { //说明index更长
		length_after_trim = 1
		rf.logs = make([]LogEntry, 1)
	} else {
		newLogs := make([]LogEntry, length_after_trim)
		copy(newLogs, rf.logs[index-rf.offset:]) //多保留一个位置
		rf.logs = newLogs
	}
	if index > rf.lastLogIndex {
		rf.lastLogIndex = index
	}
	rf.logs[0].Term = term
	rf.offset = index
	rf.persister.SaveStateAndSnapshot(rf.getRaftState(), snapshot)
	rf.snapshot = snapshot
}

//hold lock
func (rf *Raft) getRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.offset)
	e.Encode(rf.lastLogIndex)
	e.Encode(rf.logs)
	return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.

//hold rf.mu.lock
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example
	rf.persister.SaveRaftState(rf.getRaftState())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, offset, lastLogIndex int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&offset) != nil ||
		d.Decode(&lastLogIndex) != nil {
		Logger(dError, "S%d read persist error\n", rf.me)
	} else {
		logs := make([]LogEntry, lastLogIndex-offset)
		if d.Decode(&logs) != nil {
			Logger(dError, "S%d read persist logs error\n", rf.me)
		} else {
			rf.currentTerm = currentTerm
			rf.votedFor = votedFor
			rf.offset = offset
			rf.lastLogIndex = lastLogIndex
			rf.logs = logs
			Logger(dPersist, "S%d read persist ok\n", rf.me)
		}
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	//Your code here (2D).
	rf.mu.Lock()
	if rf.lastApplied >= lastIncludedIndex {
		rf.mu.Unlock()
		Logger(dSnap, "S%v cond install false lastApplied:%d includeIndex:%d\n",
			rf.me, rf.lastApplied, lastIncludedIndex)
		return false
	}
	Logger(dSnap, "S%v cond install return true lastApplied:%d includeIndex:%d\n",
		rf.me, rf.lastApplied, lastIncludedIndex)
	rf.doPersistRaftAndSnap(lastIncludedIndex, lastIncludedTerm, snapshot)
	rf.mu.Unlock()
	return true
}

type InstallSnapArgs struct {
	//2a
	Term int
	//other
	LastIncludedIndex int
	LastIncludedTerm  int
	//log entries TODO
	Snap []byte
}
type InstallSnapReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapArgs, reply *InstallSnapReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() { reply.Term = rf.currentTerm }()

	if args.Term < rf.currentTerm {
		return
	}

	//大term leader
	if args.Term > rf.currentTerm {
		rf.toHigherTermWithLock(args.Term)
	}
	rf.role = FOLLOWER
	rf.freshTimer()
	//snapshot 本身一定一致，其内容（非本身）一定可以apply
	//承诺一定会apply到snap之后（未必安装snap）

	Logger(dSnap, "S%d recv install index:%d last%d\n", rf.me, args.LastIncludedIndex, rf.lastLogIndex)
	reply.Term = rf.currentTerm
	if rf.lastApplied >= args.LastIncludedIndex {

		Logger(dSnap, "S%d applied %d ignore install index %d \n",
			rf.me, args.LastIncludedIndex, rf.lastApplied)
		return
	}

	rf.freshTimer()
	go func() {
		Logger(dSnap, "S%d write snap to chan index %d\n",
			rf.me, args.LastIncludedIndex)
		rf.applyCh <- ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Snap,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	Logger(dSnap, "S%d term %d service snap, logs -> [%d->%d]\n", rf.me, rf.currentTerm, index+1, rf.lastLogIndex)

	term := rf.logs[index-rf.offset].Term
	rf.doPersistRaftAndSnap(index, term, snapshot)
	rf.mu.Unlock()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //2a
	CandidateId  int //2a
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //2a
	VoteGranted bool //2a
}

//
// example RequestVote RPC handler.
//
func isNewEnough(selfTerm, otherTerm, selfIndex, otherIndex int) bool {
	if otherTerm > selfTerm {
		return true
	} else if otherTerm < selfTerm {
		return false
	} else {
		return otherIndex >= selfIndex
	}
}

//hold lock
func (rf *Raft) deleteTailLogs(from int) {
	AssertTrue(from > 0 && from <= rf.lastLogIndex,
		"from:%d lastLog:%d\n", rf.me, from, rf.lastLogIndex)
	Logger(dLog1, "S%d term %d delete logs [%d->%d]\n", rf.me, rf.currentTerm, rf.lastLogIndex, from-1)
	rf.logs = append([]LogEntry{}, rf.logs[:from-rf.offset]...)
	rf.lastLogIndex = from - 1
	rf.persist()
}

//hold lock,
func (rf *Raft) appendLogs(logs []LogEntry) {

	rf.lastLogIndex += len(logs)
	Logger(dLog1, "S%d term %d + %d logs [tail->%d]\n", rf.me, rf.currentTerm, len(logs), rf.lastLogIndex)
	rf.logs = append(rf.logs, logs...)
	rf.persist()
}

//hold lock
func (rf *Raft) appendLog(log LogEntry) {
	rf.lastLogIndex += 1
	Logger(dLog1, "S%d term %d + 1 log [tail->%d]\n", rf.me, rf.currentTerm, rf.lastLogIndex)
	rf.logs = append(rf.logs, log)
	rf.persist()
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() { reply.Term = rf.currentTerm }()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.toHigherTermWithLock(args.Term)
	}
	if rf.role == LEADER {
		reply.VoteGranted = false
		Logger(dLeader, "S%d term %d leader reject vote to S%d\n",
			rf.me, rf.currentTerm, args.CandidateId)
		return
	}

	reply.Term = rf.currentTerm
	selfIndex := rf.lastLogIndex
	selfTerm := rf.logs[selfIndex-rf.offset].Term

	if rf.votedFor == args.CandidateId ||
		(rf.votedFor == -1 &&
			isNewEnough(selfTerm, args.LastLogTerm, selfIndex, args.LastLogIndex)) {
		reply.VoteGranted = true
		rf.freshTimer()
		rf.votedFor = args.CandidateId
		Logger(dVote, "S%d term %d vote to S%d \n",
			rf.me, rf.currentTerm, args.CandidateId)
		rf.persist()
	} else {
		reply.VoteGranted = false
		if rf.votedFor != -1 {
			Logger(dVote, "S%d term %d decline S%d for voted S%d \n",
				rf.me, rf.currentTerm, args.CandidateId, rf.votedFor)
		} else {
			Logger(dVote, "S%d term %d decline S%d not new self[t%d,i%d] his[t%d,i%d] \n",
				rf.me, rf.currentTerm, args.CandidateId,
				selfTerm, selfIndex, args.LastLogTerm, args.LastLogIndex)
		}
	}
}

//with lock and hold lock
func (rf *Raft) toHigherTermWithLock(term int) {
	AssertTrue(rf.currentTerm < term, "tohigherTerm %d->%d\n", rf.me, rf.currentTerm, term)
	Logger(dRole, "S%d ----> follow %d->%d\n", rf.me, rf.currentTerm, term)
	rf.role = FOLLOWER
	rf.votes = 0
	//persist 2c
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

type AppendArgs struct {
	//2a
	Term     int
	LeaderId int
	//other
	PrevLogIndex int
	PrevLogTerm  int
	//log entries TODO
	Entries      []LogEntry
	LeaderCommit int
}
type AppendReply struct {
	Term           int
	Success        bool
	RejectedByTerm bool
	NextIndex      int
	XTerm          int
	XIndex         int
	XLen           int
}

//hold lock
func (rf *Raft) findTermFirstIndex(from int) int {
	i := from - 1
	term := rf.logs[from-rf.offset].Term
	for i > rf.offset {
		if rf.logs[i-rf.offset].Term != term {
			break
		}
		i--
	}
	i++
	AssertTrue(i >= rf.offset && rf.logs[i-rf.offset].Term == rf.logs[from-rf.offset].Term,
		"must equal,found i:%d Term:%d, from i:%d, Term:%d\n",
		rf.me, i, rf.logs[i-rf.offset].Term, from, rf.logs[from-rf.offset].Term)
	return i
}
func (rf *Raft) Append(args *AppendArgs, reply *AppendReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() { reply.Term = rf.currentTerm }()
	//next index在false且term小于等于对方时启用
	Logger(dLog2, "S%d term %d recv append \n", rf.me, rf.currentTerm)
	reply.Success = false
	reply.RejectedByTerm = false
	if args.Term < rf.currentTerm {
		reply.RejectedByTerm = true
		return
	}

	defer rf.freshTimer()
	if args.Term > rf.currentTerm {
		rf.toHigherTermWithLock(args.Term)
	} else {
		rf.role = FOLLOWER
	}
	leaderSendLastIndex := args.PrevLogIndex + len(args.Entries)
	reply.NextIndex = leaderSendLastIndex + 1

	//too short case 3
	if args.PrevLogIndex > rf.lastLogIndex {
		reply.XTerm = -1
		reply.XLen = rf.lastLogIndex + 1
		Logger(dLog2, "S%d term %d lastIndex %d, %d's pre [t%d,i%d] too late\n",
			rf.me, rf.currentTerm, rf.lastLogIndex, args.LeaderId, args.PrevLogTerm, args.PrevLogIndex)
		return
	}

	//前缀冲突，case 1 && case 2快速回退
	if args.PrevLogIndex > rf.offset && rf.logs[args.PrevLogIndex-rf.offset].Term != args.PrevLogTerm {

		Logger(dLog2, "S%d term %d log decline S%d term %d pre[t%d,i%d],for last log's term:%d i:%d\n",
			rf.me, rf.currentTerm, args.LeaderId, args.Term, args.PrevLogTerm, args.PrevLogIndex,
			rf.logs[rf.lastLogIndex-rf.offset].Term, rf.lastLogIndex)

		reply.XLen = rf.lastLogIndex + 1
		reply.XTerm = rf.logs[args.PrevLogIndex-rf.offset].Term
		reply.XIndex = rf.findTermFirstIndex(args.PrevLogIndex)

		Logger(dLog2, "S%d term %d return xlen%d xterm%d xindex%d\n",
			rf.me, rf.currentTerm, reply.XLen, reply.XTerm, reply.XIndex)
		return
	}

	//前缀匹配
	reply.Success = true

	reply.NextIndex = leaderSendLastIndex + 1
	if leaderSendLastIndex <= rf.lastApplied {
		Logger(dLog2, "S%d already applied %d %d or heartbeats\n",
			rf.me, rf.lastApplied, leaderSendLastIndex)
		return
	}

	scan_end := rf.lastLogIndex
	if scan_end > leaderSendLastIndex {
		scan_end = leaderSendLastIndex
	}
	scan_from := args.PrevLogIndex + 1
	if scan_from <= rf.offset {
		scan_from = rf.offset + 1
	}
	if scan_from <= scan_end {
		for scan_from <= scan_end {
			if rf.logs[scan_from-rf.offset].Term !=
				args.Entries[scan_from-args.PrevLogIndex-1].Term {
				rf.deleteTailLogs(scan_from)
				break
			}
			scan_from++
		}
	}

	if scan_from <= leaderSendLastIndex {
		rf.appendLogs(args.Entries[scan_from-args.PrevLogIndex-1:])
	}

	to_commit := args.LeaderCommit
	if to_commit > rf.lastLogIndex {
		to_commit = rf.lastLogIndex
	}
	if to_commit > rf.commitIndex {
		Logger(dCommit, "S%d term %d commit (%d->%d]\n",
			rf.me, rf.currentTerm, rf.commitIndex, to_commit)
		rf.commitIndex = to_commit
		if rf.commitIndex > rf.lastApplied {
			Logger(dApply, "S%d signal sent\n", rf.me)
			rf.applyCond.Signal()
		}

	}

	Logger(dLog2, "S%d term %d expect nextindex %d\n", rf.me, rf.currentTerm, reply.NextIndex)

}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()

	term = rf.currentTerm
	isLeader = rf.role == LEADER
	if !isLeader || rf.killed() {
		rf.mu.Unlock()
		return 0, term, false
	}
	term = rf.currentTerm

	rf.appendLog(LogEntry{
		Term:    term,
		Command: command,
	})
	index = rf.lastLogIndex
	Logger(dClient, "S%d term %d request of index %d\n", rf.me, term, index)
	rf.mu.Unlock()

	go func() {
		time.Sleep(3 * time.Millisecond)
		rf.clientReqCond.Broadcast()
	}()

	return index, term, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	Logger(dLog3, "S%d killed########################\n\n", rf.me)
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
	for !rf.killed() {
		if rf.checkTimeOut() {
			go rf.newElection()
			rf.freshTimer()
		}
		rf.sleepTimeout()
	}
}

func (rf *Raft) applier() {
	rf.mu.Lock()

	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.lastApplied < rf.commitIndex {

			rf.lastApplied++
			Logger(dApply, "S%d term %d apply %d\n", rf.me, rf.currentTerm, rf.lastApplied)
			args := ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				Command:       rf.logs[rf.lastApplied-rf.offset].Command,
				CommandIndex:  rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- args
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}

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
	//init
	rf := &Raft{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.clientReqCond = sync.NewCond(&rf.mu)
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.peerCnt = len(rf.peers)
	rf.major = (rf.peerCnt + 1) / 2
	rf.role = FOLLOWER
	rf.applyCh = applyCh
	rf.freshTimer()
	rf.nextIndex = make([]int, rf.peerCnt)
	rf.matchIndex = make([]int, rf.peerCnt)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.logs = make([]LogEntry, 1)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.offset = 0
	rf.lastLogIndex = 0
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	rf.commitIndex = rf.offset
	rf.lastApplied = rf.offset
	Logger(dPersist, "S%d init from snap offset %d \n", rf.me, rf.offset)
	// start ticker goroutine to start elections
	go rf.applier()
	go rf.ticker()
	return rf
}
