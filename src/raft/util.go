package raft

import (
	"fmt"
	"math/rand"
	"time"

	logger "6.824/raft-logs"
)

func (rf *Raft) AssertTrue(test bool, format string, a ...interface{}) {
	if !test {
		panic(fmt.Sprintf(fmt.Sprintf("S%d ", rf.me)+format, a...))
	}
}

func getRandomElectionTimeout() time.Duration {
	return time.Duration(rand.Int())%RANDOM_PLUS + ELECTION_TIMEOUT
}

//hold lock
func (rf *Raft) ind(index int) int {
	return index - rf.offset
}

func (rf *Raft) checkTimeOut() bool {
	rf.timerLock.Lock()
	defer rf.timerLock.Unlock()
	return rf.timeDdl.Before(time.Now())
}
func (rf *Raft) sleepTimeout() {
	rf.timerLock.Lock()
	ddl := rf.timeDdl
	rf.timerLock.Unlock()
	now := time.Now()
	if ddl.After(now) {
		time.Sleep(ddl.Sub(now))
	}
}

//hold lock(rf.mu)
func (rf *Raft) freshTimer() {
	rf.timerLock.Lock()
	rf.logger.L(logger.Timer, "timer freshed\n")
	defer rf.timerLock.Unlock()
	rf.timeDdl = time.Now().Add(getRandomElectionTimeout())
}

//send RPC
func (rf *Raft) sendAppend(server int, args *AppendArgs, reply *AppendReply) bool {
	ok := rf.peers[server].Call("Raft.Append", args, reply)
	return ok
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapArgs, reply *InstallSnapReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
