package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func getRandomElectionTimeout(server, peerCnt int) time.Duration {
	return RANDOM_PLUS*time.Duration(server)/time.Duration(peerCnt) + ELECTION_TIMEOUT
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

//with lock
func (rf *Raft) freshTimer() {
	rf.timerLock.Lock()
	Logger(dTimer, "S%d timer freshed\n", rf.me)
	defer rf.timerLock.Unlock()
	rf.timeDdl = time.Now().Add(getRandomElectionTimeout(rf.me, rf.peerCnt))
}
