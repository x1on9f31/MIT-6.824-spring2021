package raft

import (
	// "net/http"
	// _ "net/http/pprof"
	"sort"
	"time"

	logger "6.824/raft-logs"
)

// func init() {
// 	go func() {
// 		err := http.ListenAndServe(":9999", nil)
// 		if err != nil {
// 			panic(err)
// 		}
// 	}()
// }

func (rf *Raft) doLeaderThing(term int) {
	rf.mu.Lock()
	if rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	// rf.appendOneLog(LogEntry{
	// 	Term:    rf.currentTerm,
	// 	Command: nil,
	// })
	heartBeatsTimer := time.NewTimer(HEART_INTERVAL)
	done := make(chan bool, rf.peerCnt-1)
	go func() {
		for {
			select {
			case <-heartBeatsTimer.C:
				rf.wakeLeaderCond.Broadcast()
				heartBeatsTimer.Reset(HEART_INTERVAL)
			case <-done:
				rf.logger.L(logger.Timer, "term %d leader stop heartTimer\n", term)
				rf.wakeLeaderCond.Broadcast()
				return
			}
		}
	}()

	rf.spawnPeerSyncers(term, done)
	rf.mu.Unlock()
}

//hold lock
func (rf *Raft) spawnPeerSyncers(term int, done chan bool) {
	rf.logger.L(logger.Leader, "term %d leader start n threads", term)
	for i := 0; i < rf.peerCnt; i++ {
		if i != rf.me {
			args := AppendArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: rf.lastLogIndex,
				PrevLogTerm:  rf.logs[rf.lastLogIndex-rf.offset].Term,
				Entries:      rf.logs[0:0],
				LeaderCommit: rf.commitIndex,
			}
			go func(peer int) { //handle single raft peer
				go rf.doAppendRPC(peer, term, &args) //sync once just after become new leader
				rf.mu.Lock()
				defer func() { rf.logger.L(logger.Leader, "term %d leader for S%d loop closed\n", term, peer) }()
				defer rf.mu.Unlock()
				defer func() { done <- true }()

				//leader big loop
				for !rf.killed() {

					if rf.currentTerm != term || rf.role != LEADER {
						return
					}
					rf.logger.L(logger.Leader, "term %d leader for S%d iter\n", term, peer)

					if rf.lastLogIndex < rf.nextIndex[peer] {
						//no more logs to send, just do it as sending heartbeats
						//after it, wait util new logs or heartbeats timer signal come
						prevLogIndex := rf.nextIndex[peer] - 1
						prevLogTerm := rf.logs[prevLogIndex-rf.offset].Term
						arg := AppendArgs{Term: term, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: []LogEntry{}, LeaderCommit: rf.commitIndex}
						go rf.doAppendRPC(peer, term, &arg)
					} else {
						//lastLogIndex >= prevLogIndex + 1
						//now we must send logs or send install snapshot
						rf.AssertTrue(rf.nextIndex[peer] > rf.matchIndex[peer], "term %d for S%d next %d match %d\n",
							term, peer, rf.nextIndex[peer], rf.matchIndex[peer])

						prevLogIndex := rf.nextIndex[peer] - 1
						if prevLogIndex < rf.offset {
							arg := InstallSnapArgs{Term: term, LastIncludedIndex: rf.offset, LastIncludedTerm: rf.logs[0].Term, Snap: rf.snapshot}
							go rf.doInstallRPC(peer, term, &arg)
						} else {
							prevLogTerm := rf.logs[prevLogIndex-rf.offset].Term
							arg := AppendArgs{Term: term, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: rf.logs[prevLogIndex+1-rf.offset:], LeaderCommit: rf.commitIndex}
							go rf.doAppendRPC(peer, term, &arg)
						}
					}
					rf.wakeLeaderCond.Wait()
				}

			}(i)
		}
	}
}

func getKth(c []int, k int) int {
	sort.Ints(c)
	return c[k]
}

//hold lock ,role:LEADER
func (rf *Raft) appendOkAsLeader(nextIndex, peer int, isAppend bool) {

	if isAppend {
		rf.logger.L(logger.Leader, "term %d leader append ok to S%d,nextIndex %d\n",
			rf.currentTerm, peer, nextIndex)
	} else {
		rf.logger.L(logger.Leader, "term %d leader install ok to S%d,nextIndex %d\n",
			rf.currentTerm, peer, nextIndex)
	}

	rf.AssertTrue(rf.nextIndex[peer] > rf.matchIndex[peer], "term %d for S%d next %d match %d\n",
		rf.currentTerm, peer, rf.nextIndex[peer], rf.matchIndex[peer])

	if rf.matchIndex[peer] > nextIndex-1 {
		rf.logger.L(logger.Leader, "term %d leader reject append ok: next %d, match %d\n ",
			rf.currentTerm, nextIndex, rf.matchIndex[peer])
		return
	} else if rf.matchIndex[peer] == nextIndex-1 {
		rf.logger.L(logger.Leader, "term %d leader recv S%d heart %d[] reply\n",
			rf.currentTerm, peer, nextIndex-1)
	}

	rf.nextIndex[peer] = nextIndex
	rf.matchIndex[peer] = nextIndex - 1

	//find major match: the maximum index that there are majority raft peers
	//whose matchIndex equals or greater than index
	kth := rf.peerCnt / 2
	to_sort := make([]int, rf.peerCnt)
	copy(to_sort, rf.matchIndex)
	to_sort[rf.me] = rf.lastLogIndex
	major_match := getKth(to_sort, kth)

	if major_match > rf.lastLogIndex {

		rf.logger.L(logger.Leader, "term:%d , major_match:%d  > lastLogIndex:%d, S%d  matchIndex:%d nextIndex:%d->%d\n",
			rf.currentTerm, major_match, rf.lastLogIndex, peer, rf.matchIndex,
			rf.nextIndex, nextIndex)

		panic("leader")
	}

	//try to commit
	if major_match > rf.commitIndex &&
		rf.logs[major_match-rf.offset].Term == rf.currentTerm {

		if major_match == rf.commitIndex+1 {
			rf.logger.L(logger.Commit, "term %d leader commit [%d]\n",
				rf.currentTerm, major_match)
		} else {
			rf.logger.L(logger.Commit, "term %d leader commit [%d->%d]\n",
				rf.currentTerm, rf.commitIndex+1, major_match)
		}

		rf.commitIndex = major_match
		rf.applyCond.Signal()
	}

}

//hold lock, if leader has logs with xTerm, return true and the last Index of logs with that term
//else return false
func (rf *Raft) hasTermAndLastIndex(xTerm int) (bool, int) {
	i := rf.lastLogIndex
	has := false
	for i > rf.offset {
		if rf.logs[i-rf.offset].Term == xTerm {
			has = true
		} else if rf.logs[i-rf.offset].Term < xTerm {
			break
		}
		i--
	}
	if !has {
		return false, 0
	}
	return true, i + 1
}

//hold lock, use appendRPC's reply to quickly rollback
func (rf *Raft) getNextIndex(xTerm, xIndex, xLen int) int {
	if xTerm == -1 {
		return xLen
	}

	has, index := rf.hasTermAndLastIndex(xTerm)
	if !has {
		return xIndex
	}
	return index

}

//send request to raft peers
func (rf *Raft) doAppendRPC(peer, term int, args *AppendArgs) {
	if len(args.Entries) == 0 {
		rf.logger.L(logger.Leader, "term %d leader append to S%d []\n",
			term, peer)
	} else if len(args.Entries) == 1 {
		rf.logger.L(logger.Leader, "term %d leader append to S%d [%d]\n",
			term, peer,
			args.PrevLogIndex+1)
	} else {
		rf.logger.L(logger.Leader, "term %d leader append to S%d [%d->%d]\n",
			term, peer, args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries))
	}

	reply := AppendReply{
		Term:      0,
		Success:   false,
		NextIndex: 1,
	}
	ok := rf.sendAppend(peer, args, &reply)
	if !ok || rf.killed() {
		return
	}

	rf.checkAppendRPC(term, peer, &reply)

}

func (rf *Raft) doInstallRPC(peer, term int, args *InstallSnapArgs) {

	reply := InstallSnapReply{
		Term: 0,
	}

	rf.logger.L(logger.Leader, "term %d leader install snap to S%d,offset %d\n",
		term, peer, args.LastIncludedIndex)
	ok := rf.sendInstallSnapshot(peer, args, &reply)
	if !ok || rf.killed() {
		return
	}

	rf.checkInstallRPC(term, peer, args.LastIncludedIndex+1, &reply)

}

//check reply result
func (rf *Raft) checkInstallRPC(term, peer, nextIndex int, reply *InstallSnapReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.L(logger.Leader, "term %d got S%d term %d installSnap reply nextIndex %d\n",
		rf.currentTerm, peer, reply.Term, nextIndex)
	if reply.Term > rf.currentTerm {
		rf.toHigherTermWithLock(reply.Term)
		return
	}

	if rf.currentTerm != term || rf.role != LEADER || reply.Term < rf.currentTerm {
		return
	}
	if nextIndex <= rf.matchIndex[peer] {
		return
	}
	rf.nextIndex[peer] = nextIndex
}

func (rf *Raft) checkAppendRPC(term, peer int, reply *AppendReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logger.L(logger.Leader, "term %d got S%d term %d append reply\n",
		term, peer, reply.Term)

	if reply.Term > rf.currentTerm {
		rf.toHigherTermWithLock(reply.Term)
		return
	}

	if rf.currentTerm != term || rf.role != LEADER || reply.Term < rf.currentTerm || reply.RejectedByTerm {
		return
	}

	if reply.Success {
		rf.appendOkAsLeader(reply.NextIndex, peer, true)
		return
	}
	//prefix conflict
	//calculate where to rollback
	expectNextIndex := rf.getNextIndex(reply.XTerm, reply.XIndex, reply.XLen)

	//check if this is a outdated msg
	if rf.matchIndex[peer] >= expectNextIndex {
		rf.logger.L(logger.Leader, "term %d leader reject append conflict,next %d but match is %d\n ",
			term, expectNextIndex, rf.matchIndex[peer])
		return
	}

	rf.nextIndex[peer] = expectNextIndex
	rf.logger.L(logger.Leader, "term %d leader updated S%d nextIndex to %d \n",
		term, peer, expectNextIndex)

}
