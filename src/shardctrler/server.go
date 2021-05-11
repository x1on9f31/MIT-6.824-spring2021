package shardctrler

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	logger "6.824/raft-logs"
)

type Command struct {
	// Your data here.
	ClientID      int64
	Cmd_Seq       int
	OperationType int
	Operation     interface{}
}

const (
	Debug     = false
	TYPE_JOIN = iota
	TYPE_LEAVE
	TYPE_MOVE
	TYPE_QUERY
	LOGGER_IGNORE = true
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
	client_next_seq map[int64]int

	lastApplied int

	reply_chan map[int]chan bool
	logger     logger.TopicLogger
	// Your data here.
	lastConfigNum int
	configs       []Config // indexed by config num
}

func (sc *ShardCtrler) getResult(req *Command, wrongLeader bool) interface{} {
	switch req.OperationType {
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
func (sc *ShardCtrler) doRequest(req *Command) interface{} {
	sc.logger.L(logger.ServerReq, "do request args:%#v \n", req)

	//already applied
	if sc.hasResult(req.ClientID, req.Cmd_Seq) {
		sc.logger.L(logger.ServerReq, "[%3d--%d] already successed\n",
			req.ClientID%1000, req.Cmd_Seq)

		sc.mu.Unlock()
		res := sc.getResult(req, false)

		if req.OperationType == TYPE_QUERY {
			reply := res.(*QueryReply)
			reply.Config = *sc.queryConfig(req.Operation.(int))
			return reply
		}
		return res
	}

	index, _, isLeader := sc.rf.Start(*req) //do things through raft

	if !isLeader {
		sc.logger.L(logger.ServerReq, "declined [%3d--%d] for not leader\n",
			req.ClientID%1000, req.Cmd_Seq)
		sc.mu.Unlock()
		return sc.getResult(req, true)
	} else {
		sc.logger.L(logger.ServerLeader, "start [%3d--%d] as leader?\n",
			req.ClientID%1000, req.Cmd_Seq)
	}
	wait_chan := sc.getWaitChan(index)
	sc.mu.Unlock()

	result := <-wait_chan //等待

	sc.mu.Lock()
	if sc.hasResult(req.ClientID, req.Cmd_Seq) {
		sc.logger.L(logger.ServerReq, "[%3d--%d] successed !!!!! %#v\n",
			req.ClientID%1000, req.Cmd_Seq, result)
		sc.mu.Unlock()
		res := sc.getResult(req, false)
		if req.OperationType == TYPE_QUERY {
			reply := res.(*QueryReply)
			reply.Config = *sc.queryConfig(req.Operation.(int))
			return reply
		}
		return res

	} else {

		sc.logger.L(logger.ServerReq, "[%3d--%d] failed applied %#v\n",
			req.ClientID%1000, req.Cmd_Seq, result)
		sc.mu.Unlock()
		return sc.getResult(req, true)
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
func (sc *ShardCtrler) hasResult(ClientID int64, Cmd_Seq int) bool {
	if sc.client_next_seq[ClientID] > Cmd_Seq {
		return true
	}
	return false
}

//hold lock, get a channel to read result
func (sc *ShardCtrler) getWaitChan(index int) chan bool {
	if sc.reply_chan[index] == nil {
		sc.reply_chan[index] = make(chan bool, 1)
	}
	return sc.reply_chan[index]
}

//recv ApplyMsg from applyCh
func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		m := <-sc.applyCh
		sc.mu.Lock()
		if m.SnapshotValid { //snapshot
			sc.logger.L(logger.ServerSnap, "recv Installsnapshot %v %v\n", m.SnapshotIndex, sc.lastApplied)
			if sc.rf.CondInstallSnapshot(m.SnapshotTerm,
				m.SnapshotIndex, m.Snapshot) {
				old_apply := sc.lastApplied
				sc.logger.L(logger.ServerSnap, "decide Installsnapshot %v <- %v\n", m.SnapshotIndex, sc.lastApplied)
				sc.applyInstallSnapshot(m.Snapshot)
				for i := old_apply + 1; i <= m.SnapshotIndex; i++ {
					if sc.reply_chan[m.CommandIndex] != nil {
						sc.reply_chan[m.CommandIndex] <- true
					}
				}
			}
		} else if m.CommandValid && m.CommandIndex == 1+sc.lastApplied {
			sc.logger.L(logger.ServerApply, "apply %d %#v lastApplied %v\n", m.CommandIndex, m.Command, sc.lastApplied)

			sc.lastApplied = m.CommandIndex

			v, ok := m.Command.(Command)
			if !ok {
				//err
				return
			}
			sc.applyCommand(v) //may ignore duplicate cmd
			sc.lastConfigNum = len(sc.configs) - 1
			sc.logger.L(logger.ServerApply, "lastConfigNum %d last configs :%v\n", sc.lastConfigNum, sc.configs[sc.lastConfigNum])
			if sc.needSnapshot() {
				sc.doSnapshotForRaft(m.CommandIndex)
			}
			if sc.reply_chan[m.CommandIndex] != nil {
				sc.reply_chan[m.CommandIndex] <- true
			}

		} else if m.CommandValid && m.CommandIndex != 1+sc.lastApplied {
			// out of order cmd, just ignore
			sc.logger.L(logger.ServerApply, "ignore apply %v for lastApplied %v\n",
				m.CommandIndex, sc.lastApplied)
		} else {
			// wrong command
			sc.logger.L(logger.ServerApply, "Invalid apply msg\n")
		}

		sc.mu.Unlock()
	}

}
func (sc *ShardCtrler) applyCommand(v Command) {

	if sc.client_next_seq[v.ClientID] > v.Cmd_Seq {
		return
	}
	if sc.client_next_seq[v.ClientID] != v.Cmd_Seq {
		panic("cmd seq gap!!!")
	}

	sc.client_next_seq[v.ClientID]++
	//todo

	if v.OperationType != TYPE_QUERY {
		var new_config *Config
		switch v.OperationType {
		case TYPE_JOIN:
			r := v.Operation.(JoinArgs)
			new_config = sc.balanceJoin(r.Servers)
		case TYPE_LEAVE:
			r := v.Operation.([]int)
			new_config = sc.balanceLeave(r)
		case TYPE_MOVE:
			r := v.Operation.(GidShard)
			new_config = sc.moveShards(r)
		}
		sc.configs = append(sc.configs, *new_config)
	}
}

//hold lock
func (sc *ShardCtrler) applyInstallSnapshot(snap []byte) {
	if snap == nil || len(snap) < 1 { // bootstrap without any state?
		sc.logger.L(logger.ServerSnap, "empty snap\n")
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
		sc.logger.L(logger.ServerSnap, "apply install decode err\n")
		panic("err decode snap")
	} else {
		sc.lastApplied = lastIndex
		sc.client_next_seq = client_to_nextseq_map
		sc.configs = configs
	}

}

//hold lock
func (sc *ShardCtrler) doSnapshotForRaft(index int) {
	sc.logger.L(logger.ServerSnap, "do snapshot for raft %v %v\n", index, sc.lastApplied)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// v := m.Command
	// e.Encode(v)
	lastIndex := sc.lastApplied
	e.Encode(lastIndex)
	e.Encode(sc.client_next_seq)
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
		sc.logger.L(logger.ServerSnapSize, "used size: %d / %d \n", size, sc.maxraftstate)
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
	labgob.Register(GidShard{})
	sc := new(ShardCtrler)
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.me = me
	sc.logger = logger.TopicLogger{
		Me: sc.me,
	}
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.maxraftstate = -1
	sc.persister = persister

	sc.applyCh = make(chan raft.ApplyMsg, 30)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.reply_chan = make(map[int]chan bool)
	// Your code here.
	sc.lastApplied = 0
	sc.client_next_seq = make(map[int64]int)
	snap := sc.persister.ReadSnapshot()
	sc.applyInstallSnapshot(snap)

	go sc.applier()
	return sc
}
