package kvraft

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

const (
	Debug       = false
	TYPE_GET    = 0
	TYPE_PUT    = 1
	TYPE_APPEND = 2
	TYPE_OTHER  = 3
)

type Command struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	Seq      int
	OptType  int
	Opt      interface{} //not reference
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big
	persister    *raft.Persister
	// Your definitions here.
	next_seq    map[int64]int
	kv_map      map[string]string
	lastApplied int
	logger      logger.TopicLogger
	reply_chan  map[int]chan bool
}

func (kv *KVServer) getReplyStruct(req *Command, err Err) interface{} {
	switch req.OptType {
	case TYPE_APPEND:
		fallthrough
	case TYPE_PUT:
		return &PutAppendReply{
			Err: err,
		}
	case TYPE_GET:
		reply := &GetReply{
			Err: err,
		}
		if err == OK {
			reply.Value = kv.kv_map[req.Opt.(string)]
		}
		return reply
	}
	return nil
}

//hold lock,check if there is an avaliable result
func (kv *KVServer) hasResult(clientID int64, seq int) bool {
	return kv.next_seq[clientID] > seq
}

//return reference type
func (kv *KVServer) doRequest(command *Command) interface{} {

	kv.mu.Lock()
	kv.logger.L(logger.ServerReq, "do request args:%#v \n", command)

	//already applied
	if kv.hasResult(command.ClientID, command.Seq) {
		kv.logger.L(logger.ServerReq, "[%3d--%d] already successed\n",
			command.ClientID%1000, command.Seq)

		kv.mu.Unlock()
		return kv.getReplyStruct(command, OK)
	}

	index, _, isLeader := kv.rf.Start(*command)

	if !isLeader {
		kv.logger.L(logger.ServerReq, "declined [%3d--%d] for not leader\n",
			command.ClientID%1000, command.Seq)
		kv.mu.Unlock()
		return kv.getReplyStruct(command, ErrWrongLeader)
	} else {
		kv.logger.L(logger.ServerStart, "start [%3d--%d] as leader?\n",
			command.ClientID%1000, command.Seq)
	}
	wait_chan := kv.getWaitChan(index)
	kv.mu.Unlock()

	timeout := time.NewTimer(time.Millisecond * 200)
	select {
	case <-wait_chan:
	case <-timeout.C:
	}

	kv.mu.Lock()
	if kv.hasResult(command.ClientID, command.Seq) {
		kv.logger.L(logger.ServerReq, "[%3d--%d] successed !!!!! \n",
			command.ClientID%1000, command.Seq)
		kv.mu.Unlock()
		return kv.getReplyStruct(command, OK)
	} else {

		kv.logger.L(logger.ServerReq, "[%3d--%d] failed applied \n",
			command.ClientID%1000, command.Seq)
		kv.mu.Unlock()
		return kv.getReplyStruct(command, ErrNoKey)
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
func (kv *KVServer) Kill() {
	kv.logger.L(logger.ServerShutdown, "server killed######\n")
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//hold lock, get a channel to read result
func (kv *KVServer) getWaitChan(index int) chan bool {
	if _, ok := kv.reply_chan[index]; !ok {
		kv.reply_chan[index] = make(chan bool)
	}
	return kv.reply_chan[index]
}
func (kv *KVServer) notify(index int) {
	if c, ok := kv.reply_chan[index]; ok {
		close(c)
		delete(kv.reply_chan, index)
	}
}

//recv ApplyMsg from applyCh
func (kv *KVServer) applier() {
	for !kv.killed() {
		m := <-kv.applyCh
		kv.mu.Lock()
		if m.SnapshotValid { //snapshot
			kv.logger.L(logger.ServerSnap, "recv Installsnapshot %v %v\n", m.SnapshotIndex, kv.lastApplied)
			if kv.rf.CondInstallSnapshot(m.SnapshotTerm,
				m.SnapshotIndex, m.Snapshot) {
				old_apply := kv.lastApplied
				kv.logger.L(logger.ServerSnap, "decide Installsnapshot %v <- %v\n", m.SnapshotIndex, kv.lastApplied)
				kv.applyInstallSnapshot(m.Snapshot)
				for i := old_apply + 1; i <= m.SnapshotIndex; i++ {
					kv.notify(i)
				}
			}
		} else if m.CommandValid && m.CommandIndex == 1+kv.lastApplied {
			kv.logger.L(logger.ServerApply, "apply %d %#v lastApplied %v\n", m.CommandIndex, m.Command, kv.lastApplied)

			kv.lastApplied = m.CommandIndex
			v, ok := m.Command.(Command)
			if !ok {
				//err
				panic("not ok assertion in apply!")
			}
			kv.applyCommand(v) //may ignore duplicate cmd

			if kv.needSnapshot() {
				kv.doSnapshotForRaft(m.CommandIndex)
			}
			kv.notify(m.CommandIndex)

		} else if m.CommandValid && m.CommandIndex != 1+kv.lastApplied {
			// out of order cmd, just ignore
			kv.logger.L(logger.ServerApply, "ignore apply %v for lastApplied %v\n",
				m.CommandIndex, kv.lastApplied)
		} else {
			// wrong command
			kv.logger.L(logger.ServerApply, "Invalid apply msg\n")
		}

		kv.mu.Unlock()
	}

}
func (kv *KVServer) applyCommand(v Command) {

	if kv.next_seq[v.ClientID] > v.Seq {
		return
	}
	if kv.next_seq[v.ClientID] != v.Seq {
		panic("cmd seq gap!!!")
	}

	kv.next_seq[v.ClientID]++
	if v.OptType == TYPE_PUT || v.OptType == TYPE_APPEND {
		keyValue := v.Opt.(KeyValue)
		if v.OptType == TYPE_PUT {
			kv.kv_map[keyValue.Key] = keyValue.Value
		} else if v.OptType == TYPE_APPEND {
			kv.kv_map[keyValue.Key] += keyValue.Value
		}
	}

}

//hold lock
func (kv *KVServer) applyInstallSnapshot(snap []byte) {
	if snap == nil || len(snap) < 1 { // bootstrap without any state?
		kv.logger.L(logger.ServerSnap, "empty snap\n")
		return
	}

	r := bytes.NewBuffer(snap)
	d := labgob.NewDecoder(r)
	lastIndex := 0
	client_to_nextseq_map := make(map[int64]int)
	kv_map := make(map[string]string)
	if d.Decode(&lastIndex) != nil ||
		d.Decode(&client_to_nextseq_map) != nil ||
		d.Decode(&kv_map) != nil {
		kv.logger.L(logger.ServerSnap, "apply install decode err\n")
		panic("err decode snap")
	} else {
		kv.lastApplied = lastIndex
		kv.next_seq = client_to_nextseq_map
		kv.kv_map = kv_map
	}

}

//hold lock
func (kv *KVServer) doSnapshotForRaft(index int) {
	kv.logger.L(logger.ServerSnap, "do snapshot for raft %v %v\n", index, kv.lastApplied)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// v := m.Command
	// e.Encode(v)
	lastIndex := kv.lastApplied
	e.Encode(lastIndex)
	e.Encode(kv.next_seq)
	e.Encode(kv.kv_map)
	kv.rf.Snapshot(index, w.Bytes())
}

//hold lock
func (kv *KVServer) needSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	size := kv.persister.RaftStateSize()
	if size >= kv.maxraftstate {
		kv.logger.L(logger.ServerSnapSize, "used size: %d / %d \n", size, kv.maxraftstate)
		return true
	}
	return false
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(KeyValue{})

	kv := &KVServer{
		me: me,
		logger: logger.TopicLogger{
			Me: me,
		},
		maxraftstate: maxraftstate,
		persister:    persister,
		applyCh:      make(chan raft.ApplyMsg, 30),
		reply_chan:   make(map[int]chan bool),
		lastApplied:  0,
		kv_map:       make(map[string]string),
		next_seq:     make(map[int64]int),
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// You may need initialization code here.
	// You may need initialization code here.
	snap := kv.persister.ReadSnapshot()
	kv.applyInstallSnapshot(snap)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.applier()
	return kv
}
