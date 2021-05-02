package raft

import (
	// "net/http"
	// _ "net/http/pprof"
	"sort"
	"time"
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

	heartBeatsTimer := time.NewTimer(HEART_INTERVAL)
	done := make(chan bool, rf.peerCnt-1)
	go func() {
		for {
			select {
			case <-heartBeatsTimer.C:
				rf.clientReqCond.Broadcast()
				heartBeatsTimer.Reset(HEART_INTERVAL)
			case <-done:
				rf.DTimer("term %d leader stop heartTimer\n", term)
				rf.clientReqCond.Broadcast()
				return
			}
		}
	}()

	rf.parallelLeader(term, done)
	rf.mu.Unlock()
}

//hold lock
func (rf *Raft) parallelLeader(term int, done chan bool) {
	rf.DLeader("term %d leader start n threads", term)
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
			go func(server int) { //负责单个server
				go rf.doAppendRPC(server, term, &args) //刚上任发一次
				rf.mu.Lock()
				defer func() { rf.DLeader("term %d leader for S%d loop closed\n", term, server) }()
				defer rf.mu.Unlock()
				defer func() { done <- true }()

				//大循环
				for !rf.killed() {

					if rf.currentTerm != term || rf.role != LEADER {
						return
					}
					rf.DLeader("term %d leader for S%d iter\n", term, server)

					if rf.lastLogIndex < rf.nextIndex[server] { //大 1,空闲发送心跳，发完等待下一次
						prevLogIndex := rf.nextIndex[server] - 1
						prevLogTerm := rf.logs[prevLogIndex-rf.offset].Term
						arg := AppendArgs{Term: term, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: []LogEntry{}, LeaderCommit: rf.commitIndex}
						go rf.doAppendRPC(server, term, &arg)
						rf.clientReqCond.Wait()
						continue
					}

					//lastLogIndex >= prevLogIndex + 1
					rf.AssertTrue(rf.nextIndex[server] > rf.matchIndex[server], "term %d for S%d next %d match %d\n",
						server, rf.nextIndex[server], rf.matchIndex[server])

					prevLogIndex := rf.nextIndex[server] - 1
					if prevLogIndex < rf.offset {
						arg := InstallSnapArgs{Term: term, LastIncludedIndex: rf.offset, LastIncludedTerm: rf.logs[0].Term, Snap: rf.snapshot}

						go rf.doInstallRPC(server, term, &arg)
					} else {
						prevLogTerm := rf.logs[prevLogIndex-rf.offset].Term
						arg := AppendArgs{Term: term, LeaderId: rf.me, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, Entries: rf.logs[prevLogIndex+1-rf.offset:], LeaderCommit: rf.commitIndex}
						go rf.doAppendRPC(server, term, &arg)
					}
					rf.mu.Unlock()
					time.Sleep(HEART_INTERVAL / 2)
					rf.mu.Lock()
				}

			}(i)
		}
	}
}

func (rf *Raft) sendAppend(server int, args *AppendArgs, reply *AppendReply) bool {
	ok := rf.peers[server].Call("Raft.Append", args, reply)
	return ok
}

//todo
func getKth(c []int, k int) int {
	sort.Ints(c)
	return c[k]
}

//hold lock ,role:LEADER
func (rf *Raft) appendOkAsLeader(nextIndex, server int, isAppend bool) {
	if isAppend {
		rf.DLeader("term %d leader append ok to S%d,nextIndex %d\n",
			rf.currentTerm, server, nextIndex)
	} else {
		rf.DLeader("term %d leader install ok to S%d,nextIndex %d\n",
			rf.currentTerm, server, nextIndex)
	}
	rf.AssertTrue(rf.nextIndex[server] > rf.matchIndex[server], "term %d for S%d next %d match %d\n",
		rf.currentTerm, server, rf.nextIndex[server], rf.matchIndex[server])

	if rf.matchIndex[server] > nextIndex-1 {
		rf.DLeader("term %d leader reject append ok: next %d, match %d\n ",
			rf.currentTerm, nextIndex, rf.matchIndex[server])
		return
	} else if rf.matchIndex[server] == nextIndex-1 {
		rf.DLeader("term %d leader recv S%d heart %d] reply\n",
			rf.currentTerm, server, nextIndex-1)
	}

	rf.nextIndex[server] = nextIndex
	rf.matchIndex[server] = nextIndex - 1 //match不能回退，拒绝旧的append ok
	kth := rf.peerCnt / 2
	to_sort := make([]int, rf.peerCnt)
	copy(to_sort, rf.matchIndex)
	to_sort[rf.me] = rf.lastLogIndex
	major_match := getKth(to_sort, kth)

	if major_match > rf.lastLogIndex {

		rf.DLeader("term:%d , major_match:%d  > lastLogIndex:%d, S%d  matchIndex:%d nextIndex:%d->%d\n",
			rf.currentTerm, major_match, rf.lastLogIndex, server, rf.matchIndex,
			rf.nextIndex, nextIndex)

		panic("leader")
	}
	if major_match > rf.commitIndex &&
		rf.logs[major_match-rf.offset].Term == rf.currentTerm {
		rf.DCommit("term %d leader commit (%d->%d]\n",
			rf.currentTerm, rf.commitIndex, major_match)
		rf.commitIndex = major_match
		rf.applyCond.Signal()
	}

}

//hold lock,may return index not of term xTerm
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
	// AssertTrue(i==rf.offset||)
	return true, i + 1
}

//hold lock, ok and not success
func (rf *Raft) getNextIndex(xTerm, xIndex, xLen int) int {
	//xterm < currentTerm
	//判断自己的lastTerm是否==xterm ，如果不相等或者已经不存在lasterm了，被snap吞了
	if xTerm == -1 {
		return xLen
	}
	has, index := rf.hasTermAndLastIndex(xTerm)
	if !has {
		return xIndex
	}
	return index

}

//drop lock
func (rf *Raft) doInstallRPC(server, term int, args *InstallSnapArgs) {

	reply := InstallSnapReply{
		Term: 0,
	}

	rf.DLeader("term %d leader install snap to S%d,offset %d\n",
		rf.currentTerm, server, rf.offset)

	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, &reply)
	if !ok || rf.killed() {
		return
	}

	rf.checkInstallRPC(term, server, args.LastIncludedIndex+1, &reply)

}
func (rf *Raft) checkInstallRPC(term, server, nextIndex int, reply *InstallSnapReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.DLeader("term %d got S%d term %d installSnap reply nextIndex %d\n",
		rf.currentTerm, server, reply.Term, nextIndex)
	if reply.Term > rf.currentTerm {
		rf.toHigherTermWithLock(reply.Term)
		return
	}

	if rf.currentTerm != term || rf.role != LEADER || reply.Term < rf.currentTerm {
		return
	}
	if nextIndex <= rf.matchIndex[server] {
		return
	}
	rf.nextIndex[server] = nextIndex
}

func (rf *Raft) checkAppendRPC(term, server int, reply *AppendReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.DLeader("term %d got S%d term %d append reply\n",
		rf.currentTerm, server, reply.Term)

	if reply.Term > rf.currentTerm {
		rf.toHigherTermWithLock(reply.Term)
		return
	}

	if rf.currentTerm != term || rf.role != LEADER || reply.Term < rf.currentTerm || reply.RejectedByTerm {
		return
	}

	if reply.Success {
		rf.appendOkAsLeader(reply.NextIndex, server, true)
		return
	}
	//前缀冲突
	//相同term，但未必足够新，可能已经接受过更新的了
	//但由于不匹配被拒绝，冲突
	expectNextIndex := rf.getNextIndex(reply.XTerm, reply.XIndex, reply.XLen)

	if rf.matchIndex[server] >= expectNextIndex { //过时了，拒绝回退
		rf.DLeader("term %d leader reject append conflict,next %d but match is %d\n ",
			rf.currentTerm, expectNextIndex, rf.matchIndex[server])
		return
	}

	rf.nextIndex[server] = expectNextIndex
	rf.DLeader("term %d leader updated S%d nextIndex to %d \n",
		rf.currentTerm, server, expectNextIndex)

}

func (rf *Raft) doAppendRPC(server, term int, args *AppendArgs) {
	rf.DLeader("term %d leader append to S%d [%d->%d)\n",
		rf.currentTerm, server, args.PrevLogIndex+1, args.PrevLogIndex+len(args.Entries)+1)

	reply := AppendReply{
		Term:      0,
		Success:   false,
		NextIndex: 1,
	}
	ok := rf.sendAppend(server, args, &reply)
	if !ok || rf.killed() {
		return
	}

	rf.checkAppendRPC(term, server, &reply)

}
