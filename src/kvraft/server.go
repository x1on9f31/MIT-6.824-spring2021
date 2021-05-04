package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const (
	Debug       = false
	TYPE_GET    = 0
	TYPE_PUT    = 1
	TYPE_APPEND = 2
	TYPE_OTHER  = 3
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Command struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	Cmd_Seq  int
	OpType   int
	Key      string
	Value    string
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

	reply_chan map[int]chan *Reply
}

//just for printing logs
func (kv *KVServer) DServer(format string, a ...interface{}) {
	raft.DLogger("SVER", kv.me, format, a...)
}

//rpc handler
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	request_arg := Request{
		OpType:   TYPE_GET,
		Key:      args.Key,
		ClientID: args.ClientID,
		Cmd_Seq:  args.Cmd_Seq,
	}

	result := kv.doRequest(&request_arg)
	reply.Err = result.Err
	reply.Value = result.Value
	kv.DServer("[%3d--%d] get return result%#v\n",
		args.ClientID%1000, args.Cmd_Seq, reply)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	request_arg := Request{
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientID,
		Cmd_Seq:  args.Cmd_Seq,
	}
	switch args.Op {
	case "Put":
		request_arg.OpType = TYPE_PUT
	case "Append":
		request_arg.OpType = TYPE_APPEND
	default:
		kv.DServer("putAppend err type %d from [%3d--%d]\n",
			args.Op, args.ClientID%1000, args.Cmd_Seq)
	}

	reply_arg := kv.doRequest(&request_arg) //wait

	reply.Err = reply_arg.Err

	kv.DServer("[%3d--%d] putAppend return result%#v\n",
		args.ClientID%1000, args.Cmd_Seq, reply)
}

func (kv *KVServer) doRequest(req *Request) *Reply {
	reply := Reply{}

	kv.mu.Lock()
	kv.DServer("do request args:%#v \n", req)

	//already applied
	if kv.checkForResult(req, &reply) {
		kv.DServer("[%3d--%d] already successed\n",
			req.ClientID%1000, req.Cmd_Seq)
		kv.mu.Unlock()
		return &reply
	}

	//not applied, need proposal
	command := Command{
		Cmd_Seq:  req.Cmd_Seq,
		ClientID: req.ClientID,
		OpType:   req.OpType,
		Key:      req.Key,
		Value:    req.Value,
	}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = "not leader"
		kv.DServer("declined [%3d--%d] for not leader\n",
			req.ClientID%1000, req.Cmd_Seq)
		kv.mu.Unlock()
		return &reply
	} else {
		kv.DServer("start [%3d--%d] as leader?\n",
			req.ClientID%1000, req.Cmd_Seq)
	}
	wait_chan := kv.getWaitChan(index)
	kv.mu.Unlock()

	result := <-wait_chan //等待

	kv.mu.Lock()
	if kv.checkForResult(req, &reply) {
		kv.DServer("[%3d--%d] successed !!!!! %#v\n",
			req.ClientID%1000, req.Cmd_Seq, result)
	} else {
		reply.Err = "failed"
		kv.DServer("[%3d--%d] failed applied %#v\n",
			req.ClientID%1000, req.Cmd_Seq, result)
	}
	kv.mu.Unlock()
	return &reply
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
	kv.DServer("server killed#############\n")
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//hold lock,check if there is an avaliable result
func (kv *KVServer) checkForResult(req *Request, reply *Reply) bool {
	if kv.client_next_seq[req.ClientID] > req.Cmd_Seq {
		if req.OpType == TYPE_GET {
			reply.Value = kv.kv_map[req.Key]
		}
		return true
	}
	return false
}

//hold lock, get a channel to read result
func (kv *KVServer) getWaitChan(index int) chan *Reply {
	if kv.reply_chan[index] == nil {
		kv.reply_chan[index] = make(chan *Reply, 1)
	}
	return kv.reply_chan[index]
}

//recv ApplyMsg from applyCh
func (kv *KVServer) applier() {
	for !kv.killed() {
		m := <-kv.applyCh
		kv.mu.Lock()
		if m.SnapshotValid { //snapshot
			kv.DServer("recv Installsnapshot %v %v\n", m.SnapshotIndex, kv.lastApplied)
			if kv.rf.CondInstallSnapshot(m.SnapshotTerm,
				m.SnapshotIndex, m.Snapshot) {
				old_apply := kv.lastApplied
				kv.DServer("decide Installsnapshot %v <- %v\n", m.SnapshotIndex, kv.lastApplied)
				kv.applyInstallSnapshot(m.Snapshot)
				for i := old_apply + 1; i <= m.SnapshotIndex; i++ {
					if kv.reply_chan[m.CommandIndex] != nil {
						kv.reply_chan[m.CommandIndex] <- &Reply{}
					}
				}
			}
		} else if m.CommandValid && m.CommandIndex == 1+kv.lastApplied {
			kv.DServer("apply %d %#v lastApplied %v\n", m.CommandIndex, m.Command, kv.lastApplied)

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
				kv.reply_chan[m.CommandIndex] <- &Reply{}
			}

		} else if m.CommandValid && m.CommandIndex != 1+kv.lastApplied {
			// out of order cmd, just ignore
			kv.DServer("ignore apply %v for lastApplied %v\n",
				m.CommandIndex, kv.lastApplied)
		} else {
			// wrong command
			kv.DServer("Invalid apply msg\n")
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
	if v.OpType == TYPE_PUT {
		kv.kv_map[v.Key] = v.Value
	} else if v.OpType == TYPE_APPEND {
		kv.kv_map[v.Key] += v.Value
	}
}

//hold lock
func (kv *KVServer) applyInstallSnapshot(snap []byte) {
	if snap == nil || len(snap) < 1 { // bootstrap without any state?
		kv.DServer("empty snap\n")
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
		kv.DServer("apply install decode err\n")
		panic("err decode snap")
	} else {
		kv.lastApplied = lastIndex
		kv.client_next_seq = client_to_nextseq_map
		kv.kv_map = kv_map
	}

}

//hold lock
func (kv *KVServer) doSnapshotForRaft(index int) {
	kv.DServer("do snapshot for raft %v %v\n", index, kv.lastApplied)

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
	if size >= kv.maxraftstate*2/3-1 {
		kv.DServer("used size: %d / %d \n", size, kv.maxraftstate)
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

	kv := new(KVServer)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg, 30)

	// You may need initialization code here.

	kv.reply_chan = make(map[int]chan *Reply)
	kv.lastApplied = 0
	kv.kv_map = make(map[string]string)
	kv.client_next_seq = make(map[int64]int)
	snap := kv.persister.ReadSnapshot()
	kv.applyInstallSnapshot(snap)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.applier()
	return kv
}
