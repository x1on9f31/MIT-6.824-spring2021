package raft

//hold lock
func (rf *Raft) beforeBeNewLeader() {
	rf.DRole("be term %d ~~~~ Leader ~~~~~ \n", rf.currentTerm)
	rf.role = LEADER
	for i := 0; i < rf.peerCnt; i++ {
		rf.nextIndex[i] = rf.lastLogIndex + 1 //modified
		rf.matchIndex[i] = 0
	}

}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) goElection(term int, args *RequestVoteArgs) {

	for i := 0; i < rf.peerCnt; i++ {
		if i != rf.me {
			go func(peer int) {
				reply := RequestVoteReply{
					Term:        -1,
					VoteGranted: false,
				}
				ok := rf.sendRequestVote(peer, args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if ok && reply.Term > rf.currentTerm {
					rf.toHigherTermWithLock(reply.Term)
					return
				}

				if rf.currentTerm != term || rf.role != CANDIDATE || !ok || reply.Term < term {
					return
				}

				if !reply.VoteGranted {
					return
				}

				rf.votes++
				rf.DVote("term %d got a vote from S%d, now votes %d\n", term, peer, rf.votes)
				if rf.votes >= rf.major {
					rf.beforeBeNewLeader()
					go rf.doLeaderThing(rf.currentTerm)
				}
			}(i)
		}
	}

}

//drop lock
func (rf *Raft) newElection() {

	rf.mu.Lock()
	if rf.role == LEADER {
		rf.mu.Unlock()
		return
	}
	//persist 2c
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()

	term := rf.currentTerm
	rf.votes = 1
	rf.DRole("timeout be term %d ~~~~ candidate ~~~~\n", rf.currentTerm)

	rf.role = CANDIDATE
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex,
		LastLogTerm:  rf.logs[rf.ind(rf.lastLogIndex)].Term,
	}
	rf.mu.Unlock()

	rf.goElection(term, &args)

}
