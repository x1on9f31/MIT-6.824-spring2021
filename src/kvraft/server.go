package kvraft

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	logger "6.824/raft-logs"
)

const (
	Debug         = false
	TYPE_GET      = 0
	TYPE_PUT      = 1
	TYPE_APPEND   = 2
	TYPE_OTHER    = 3
	LOGGER_IGNORE = true
)

type Command struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID      int64
	Cmd_Seq       int
	OperationType int
	Operation     interface{} //not reference
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
	client_next_seq map[int64]int
	kv_map          map[string]string
	lastApplied     int
	logger          logger.TopicLogger
	reply_chan      map[int]chan bool
}

//rpc handler
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	request_arg := Command{
		OperationType: TYPE_GET,
		Operation:     args.Key,
		ClientID:      args.ClientID,
		Cmd_Seq:       args.Cmd_Seq,
	}

	result := kv.doRequest(&request_arg).(*GetReply)
	reply.Err = result.Err
	reply.Value = result.Value
	kv.logger.L(logger.ServerReq, "[%3d--%d] get return result%#v\n",
		args.ClientID%1000, args.Cmd_Seq, reply)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	request_arg := Command{
		Operation: KeyValue{
			Key:   args.Key,
			Value: args.Value,
		},
		ClientID: args.ClientID,
		Cmd_Seq:  args.Cmd_Seq,
	}
	switch args.Op {
	case "Put":
		request_arg.OperationType = TYPE_PUT
	case "Append":
		request_arg.OperationType = TYPE_APPEND
	default:
		kv.logger.L(logger.ServerReq, "putAppend err type %d from [%3d--%d]\n",
			args.Op, args.ClientID%1000, args.Cmd_Seq)
	}

	reply_arg := kv.doRequest(&request_arg).(*PutAppendReply) //wait

	reply.Err = reply_arg.Err

	kv.logger.L(logger.ServerReq, "[%3d--%d] putAppend return result%#v\n",
		args.ClientID%1000, args.Cmd_Seq, reply)
}

func (kv *KVServer) getResult(req *Command, err Err) interface{} {
	if err != OK {
		if req.OperationType == TYPE_GET {
			return &GetReply{
				Err: err,
			}
		}
		if req.OperationType == TYPE_PUT || req.OperationType == TYPE_APPEND {
			return &PutAppendReply{
				Err: err,
			}
		}
	}

	if req.OperationType == TYPE_GET {
		key, _ := req.Operation.(string)
		return &GetReply{
			Value: kv.kv_map[key],
			Err:   OK,
		}
	}
	if req.OperationType == TYPE_PUT || req.OperationType == TYPE_APPEND {
		return &PutAppendReply{
			Err: OK,
		}
	}
	return &struct{}{}
}

//return reference type
func (kv *KVServer) doRequest(req *Command) interface{} {

	kv.mu.Lock()
	kv.logger.L(logger.ServerReq, "do request args:%#v \n", req)

	//already applied
	if kv.hasResult(req.ClientID, req.Cmd_Seq) {
		kv.logger.L(logger.ServerReq, "[%3d--%d] already successed\n",
			req.ClientID%1000, req.Cmd_Seq)

		kv.mu.Unlock()
		return kv.getResult(req, OK)
	}

	index, _, isLeader := kv.rf.Start(req)
	if !isLeader {
		kv.logger.L(logger.ServerReq, "declined [%3d--%d] for not leader\n",
			req.ClientID%1000, req.Cmd_Seq)
		kv.mu.Unlock()
		return kv.getResult(req, ErrWrongLeader)
	} else {
		kv.logger.L(logger.ServerReq, "start [%3d--%d] as leader?\n",
			req.ClientID%1000, req.Cmd_Seq)
	}
	wait_chan := kv.getWaitChan(index)
	kv.mu.Unlock()

	result := <-wait_chan //等待

	kv.mu.Lock()
	if kv.hasResult(req.ClientID, req.Cmd_Seq) {
		kv.logger.L(logger.ServerReq, "[%3d--%d] successed !!!!! %#v\n",
			req.ClientID%1000, req.Cmd_Seq, result)
		kv.mu.Unlock()
		return kv.getResult(req, OK)
	} else {

		kv.logger.L(logger.ServerReq, "[%3d--%d] failed applied %#v\n",
			req.ClientID%1000, req.Cmd_Seq, result)
		kv.mu.Unlock()
		return kv.getResult(req, ErrNoKey)
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

//hold lock,check if there is an avaliable result
func (kv *KVServer) hasResult(ClientID int64, Cmd_Seq int) bool {
	if kv.client_next_seq[ClientID] > Cmd_Seq {
		return true
	}
	return false
}

//hold lock, get a channel to read result
func (kv *KVServer) getWaitChan(index int) chan bool {
	if kv.reply_chan[index] == nil {
		kv.reply_chan[index] = make(chan bool, 1)
	}
	return kv.reply_chan[index]
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
					if kv.reply_chan[m.CommandIndex] != nil {
						kv.reply_chan[m.CommandIndex] <- true
					}
				}
			}
		} else if m.CommandValid && m.CommandIndex == 1+kv.lastApplied {
			kv.logger.L(logger.ServerApply, "apply %d %#v lastApplied %v\n", m.CommandIndex, m.Command, kv.lastApplied)

			kv.lastApplied = m.CommandIndex
			v, ok := m.Command.(Command)
			if !ok {
				//err
				return
			}
			kv.applyCommand(v) //may ignore duplicate cmd

			if kv.needSnapshot() {
				kv.doSnapshotForRaft(m.CommandIndex)
			}
			if kv.reply_chan[m.CommandIndex] != nil {
				kv.reply_chan[m.CommandIndex] <- true
			}

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

	if kv.client_next_seq[v.ClientID] > v.Cmd_Seq {
		return
	}
	if kv.client_next_seq[v.ClientID] != v.Cmd_Seq {
		panic("cmd seq gap!!!")
	}

	kv.client_next_seq[v.ClientID]++
	if v.OperationType == TYPE_PUT || v.OperationType == TYPE_APPEND {
		keyValue := v.Operation.(KeyValue)
		if v.OperationType == TYPE_PUT {
			kv.kv_map[keyValue.Key] = keyValue.Value
		} else if v.OperationType == TYPE_APPEND {
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
		kv.client_next_seq = client_to_nextseq_map
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
	e.Encode(kv.client_next_seq)
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

	kv := new(KVServer)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.me = me
	kv.logger = logger.TopicLogger{
		Me: kv.me,
	}

	kv.maxraftstate = maxraftstate
	kv.persister = persister
	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg, 30)

	// You may need initialization code here.

	kv.reply_chan = make(map[int]chan bool)
	kv.lastApplied = 0
	kv.kv_map = make(map[string]string)
	kv.client_next_seq = make(map[int64]int)
	snap := kv.persister.ReadSnapshot()
	kv.applyInstallSnapshot(snap)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.applier()
	return kv
}
