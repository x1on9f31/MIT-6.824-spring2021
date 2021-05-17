package shardctrler

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	logger "6.824/raft-logs"
)

type Command struct {
	// Your data here.
	ClientID int64
	Seq      int
	OptType  int
	Opt      interface{}
}

const (
	Debug     = false
	TYPE_JOIN = iota
	TYPE_LEAVE
	TYPE_MOVE
	TYPE_QUERY
)

type ShardCtrler struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister
	// Your definitions here.
	next_seq map[int64]int

	lastApplied int

	reply_chan map[int]chan bool
	logger     logger.TopicLogger
	// Your data here.
	configs []Config // indexed by config num
}

func (sc *ShardCtrler) getReplyStruct(optType int, wrongLeader bool) interface{} {
	switch optType {
	case TYPE_JOIN:
		return &JoinReply{
			WrongLeader: wrongLeader,
		}

	case TYPE_LEAVE:
		return &LeaveReply{
			WrongLeader: wrongLeader,
		}

	case TYPE_MOVE:
		return &MoveReply{
			WrongLeader: wrongLeader,
		}

	case TYPE_QUERY:
		return &QueryReply{
			WrongLeader: wrongLeader,
		}

	default:
		return nil
	}

}

//return reference type,drop lock
func (sc *ShardCtrler) doRequest(command *Command) interface{} {
	// sc.logger.L(logger.CtrlerReq, "do request args:%#v \n", req)

	//already applied
	if ok, res := sc.hasResult(command); ok {
		sc.mu.Unlock()
		return res
	}

	index, _, isLeader := sc.rf.Start(*command) //do things through raft

	if !isLeader {
		sc.logger.L(logger.CtrlerReq, "declined [%3d--%d] for not leader\n",
			command.ClientID%1000, command.Seq)
		sc.mu.Unlock()
		return sc.getReplyStruct(command.OptType, true)
	} else {
		sc.logger.L(logger.CtrlerStart, "start [%3d--%d] as leader?\n",
			command.ClientID%1000, command.Seq)
	}
	wait_chan := sc.getWaitChan(index)
	sc.mu.Unlock()

	timeout := time.NewTimer(time.Millisecond * 200)
	select {
	case <-wait_chan:
	case <-timeout.C:
	}

	sc.mu.Lock()
	if ok, res := sc.hasResult(command); ok {
		sc.mu.Unlock()
		return res

	} else {
		sc.logger.L(logger.CtrlerReq, "[%3d--%d] failed applied \n",
			command.ClientID%1000, command.Seq)
		sc.mu.Unlock()
		return sc.getReplyStruct(command.OptType, true)
	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (sc *ShardCtrler) Kill() {

	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

//hold lock,check if there is an avaliable result
func (sc *ShardCtrler) hasResult(req *Command) (bool, interface{}) {
	if sc.next_seq[req.ClientID] > req.Seq {

		sc.logger.L(logger.CtrlerReq, "[%3d--%d] successed !!!!!\n",
			req.ClientID%1000, req.Seq)

		res := sc.getReplyStruct(req.OptType, false)
		if req.OptType == TYPE_QUERY {
			reply := res.(*QueryReply)
			reply.Config = *sc.queryConfig(req.Opt.(int))
			return true, reply
		}
		return true, res
	}
	return false, nil
}

//hold lock, get a channel to read result
func (sc *ShardCtrler) getWaitChan(index int) chan bool {
	if _, ok := sc.reply_chan[index]; !ok {
		sc.reply_chan[index] = make(chan bool)
	}
	return sc.reply_chan[index]
}
func (sc *ShardCtrler) notify(index int) {
	if c, ok := sc.reply_chan[index]; ok {
		close(c)
		delete(sc.reply_chan, index)
	}
}

//recv ApplyMsg from applyCh
func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		m := <-sc.applyCh
		sc.mu.Lock()
		if m.SnapshotValid { //snapshot
			sc.logger.L(logger.CtrlerSnap, "ctrler recv Installsnapshot %v %v\n", m.SnapshotIndex, sc.lastApplied)
			if sc.rf.CondInstallSnapshot(m.SnapshotTerm,
				m.SnapshotIndex, m.Snapshot) {
				old_apply := sc.lastApplied
				sc.logger.L(logger.CtrlerSnap, "ctrler decide Installsnapshot %v <- %v\n", m.SnapshotIndex, sc.lastApplied)
				sc.applyInstallSnapshot(m.Snapshot)
				for i := old_apply + 1; i <= m.SnapshotIndex; i++ {
					sc.notify(i)
				}
			}
		} else if m.CommandValid && m.CommandIndex == 1+sc.lastApplied {
			//sc.logger.L(logger.CtrlerApply, "apply %d %#v lastApplied %v\n", m.CommandIndex, m.Command, sc.lastApplied)

			sc.lastApplied = m.CommandIndex

			v, ok := m.Command.(Command)
			if !ok {
				//err
			} else {
				sc.applyCommand(v) //may ignore duplicate cmd
			}

			//sc.logger.L(logger.CtrlerApply, "lastConfigNum %d last configs :%v\n", len(sc.configs)-1, sc.configs[len(sc.configs)-1])
			if sc.needSnapshot() {
				sc.doSnapshotForRaft(m.CommandIndex)
			}
			sc.notify(m.CommandIndex)

		} else if m.CommandValid && m.CommandIndex != 1+sc.lastApplied {
			// out of order cmd, just ignore
			sc.logger.L(logger.CtrlerApply, "ctrler ignore apply %v for lastApplied %v\n",
				m.CommandIndex, sc.lastApplied)
		} else {
			// wrong command
			sc.logger.L(logger.CtrlerApply, "ctrler Invalid apply msg\n")
		}

		sc.mu.Unlock()
	}

}
func (sc *ShardCtrler) applyCommand(v Command) {

	if sc.next_seq[v.ClientID] > v.Seq {
		return
	}
	if sc.next_seq[v.ClientID] != v.Seq {
		panic("ctrler cmd seq gap!!!")
	}

	sc.next_seq[v.ClientID]++

	if v.OptType != TYPE_QUERY {
		var new_config *Config
		switch v.OptType {
		case TYPE_JOIN:
			new_config = sc.doJoin(v.Opt.(JoinArgs).Servers)
		case TYPE_LEAVE:
			new_config = sc.doLeave(v.Opt.([]int))
		case TYPE_MOVE:
			new_config = sc.doMove(v.Opt.(GIDandShard))
		}
		sc.configs = append(sc.configs, *new_config)
		sc.logger.L(logger.CtrlerApply, "ctrler change to config %v\n", new_config)
	}

}

//hold lock
func (sc *ShardCtrler) applyInstallSnapshot(snap []byte) {
	if snap == nil || len(snap) < 1 { // bootstrap without any state?
		sc.logger.L(logger.CtrlerSnap, "ctrler empty snap\n")
		return
	}

	r := bytes.NewBuffer(snap)
	d := labgob.NewDecoder(r)
	lastIndex := 0
	client_to_nextseq_map := make(map[int64]int)
	configs := make([]Config, 1)
	if d.Decode(&lastIndex) != nil ||
		d.Decode(&client_to_nextseq_map) != nil ||
		d.Decode(&configs) != nil {
		sc.logger.L(logger.CtrlerSnap, "ctrler apply install decode err\n")
		panic("ctrler err decode snap")
	} else {
		sc.lastApplied = lastIndex
		sc.next_seq = client_to_nextseq_map
		sc.configs = configs
	}

}

//hold lock
func (sc *ShardCtrler) doSnapshotForRaft(index int) {
	sc.logger.L(logger.CtrlerSnap, "do snapshot for raft %v %v\n", index, sc.lastApplied)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// v := m.Command
	// e.Encode(v)
	lastIndex := sc.lastApplied
	e.Encode(lastIndex)
	e.Encode(sc.next_seq)
	e.Encode(sc.configs)
	sc.rf.Snapshot(index, w.Bytes())
}

//hold lock
func (sc *ShardCtrler) needSnapshot() bool {
	if sc.maxraftstate == -1 {
		return false
	}
	size := sc.persister.RaftStateSize()
	if size >= sc.maxraftstate {
		sc.logger.L(logger.CtrlerSnapSize, "ctrler used size: %d / %d \n", size, sc.maxraftstate)
		return true
	}
	return false
}

// needed by shardsc tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of

// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Command{})
	labgob.Register(Config{})
	labgob.Register(JoinArgs{})
	labgob.Register(GIDandShard{})
	sc := &ShardCtrler{
		me:           me,
		configs:      make([]Config, 1),
		maxraftstate: -1,
		persister:    persister,
		applyCh:      make(chan raft.ApplyMsg, 30),
		reply_chan:   make(map[int]chan bool),
		lastApplied:  0,
		next_seq:     make(map[int64]int),
		logger: logger.TopicLogger{
			Me: me,
		},
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.configs[0].Groups = map[int][]string{}
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	// Your code here.
	snap := sc.persister.ReadSnapshot()
	sc.applyInstallSnapshot(snap)
	go sc.applier()
	return sc
}
