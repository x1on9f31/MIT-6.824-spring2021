package shardkv

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	logger "6.824/raft-logs"
	"6.824/shardctrler"
)

type Config = shardctrler.Config

const (
	Debug    = false
	TYPE_GET = iota
	TYPE_PUT
	TYPE_APPEND
	TYPE_NEWCONFIG
	TYPE_MIGRATE
	TYPE_NOP = 1000
	NShards  = shardctrler.NShards
)

type Command struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	ClientID int64
	Seq      int
	OptType  int
	Opt      interface{} //not reference
	Num      int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister
	sm           *shardctrler.Clerk

	// Your definitions here.

	logger     logger.TopicLogger
	reply_chan map[int]chan bool

	//snapshot
	states        []ShardData
	lastApplied   int
	pendingShards [NShards]bool
	config        Config
}

//hold lock, get a channel to read result
func (kv *ShardKV) getWaitChan(index int) chan bool {
	if _, ok := kv.reply_chan[index]; !ok {
		kv.reply_chan[index] = make(chan bool)
	}
	return kv.reply_chan[index]
}

func (kv *ShardKV) getReplyStruct(t int, err Err) interface{} {
	switch t {
	case TYPE_APPEND:
		fallthrough
	case TYPE_PUT:
		return &PutAppendReply{
			Err: err,
		}
	case TYPE_GET:
		return &GetReply{
			Err: err,
		}
	}
	return nil
}

//hold lock,check if there is an avaliable result
func (kv *ShardKV) hasResult(clientID int64, seq, shard int) bool {
	return kv.states[shard].NextSeq[clientID] > seq
}

func (kv *ShardKV) checkResult(command *Command) (bool, bool, interface{}) {

	shard := key2shard(command.Key)

	if !kv.isResponsible(shard, command.Num) {
		return false, false, kv.getReplyStruct(command.OptType, ErrWrongGroup)
	}
	if !kv.isAvaiable(shard) {
		return false, false, kv.getReplyStruct(command.OptType, ErrWrongLeader)
	}
	//applied
	if kv.hasResult(command.ClientID, command.Seq, shard) {
		kv.logger.L(logger.ShardKVReq, "[%3d--%d] successed\n",
			command.ClientID%1000, command.Seq)

		res := kv.getReplyStruct(command.OptType, OK)
		if command.OptType == TYPE_GET {
			reply := res.(*GetReply)
			reply.Value = kv.states[shard].KVmap[command.Key]
			return true, true, reply
		}
		return true, true, res
	} else {
		return true, false, kv.getReplyStruct(command.OptType, ErrWrongLeader)
	}
}

func printCommand(command *Command) string {
	return fmt.Sprintf("Key %v type %d num %d Client %d Seq %d",
		command.Key, command.OptType, command.Num,
		command.ClientID, command.Seq)
}

//return reference type
func (kv *ShardKV) doRequest(command *Command) interface{} {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.logger.L(logger.ShardKVReq, "do request args:%s \n", printCommand(command))

	if avaliable, ok, res := kv.checkResult(command); !avaliable || ok {
		return res
	}

	index, _, isLeader := kv.rf.Start(*command)

	if !isLeader {
		kv.logger.L(logger.ShardKVReq, "declined [%3d--%d] for not leader\n",
			command.ClientID%1000, command.Seq)
		return kv.getReplyStruct(command.OptType, ErrWrongLeader)
	} else {
		kv.logger.L(logger.ShardKVStart, "start [%3d--%d] as leader?\n",
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

	avaliable, ok, res := kv.checkResult(command)
	if ok {
		kv.logger.L(logger.ShardKVReq, "[%3d--%d] ok \n",
			command.ClientID%1000, command.Seq)
	} else {
		kv.logger.L(logger.ShardKVReq, "[%3d--%d] failed applied ava %v\n",
			command.ClientID%1000, command.Seq, avaliable)
	}

	return res

}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.logger.L(logger.ShardKVShutDown, "ShardKV killed######\n")
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}
func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// ShardKVs[] contains the ports of the ShardKVs in this group.
//
// me is the index of the current ShardKV in ShardKVs[].
//
// the k/v ShardKV should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v ShardKV should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(ShardKVname) turns a ShardKV name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartShardKV() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(MigrateCommand{})
	labgob.Register(ShardData{})

	kv := &ShardKV{
		me: me,
		logger: logger.TopicLogger{
			Me: me + gid%10*100 + 100,
		},
		maxraftstate: maxraftstate,
		persister:    persister,
		applyCh:      make(chan raft.ApplyMsg, 30),
		reply_chan:   make(map[int]chan bool),
		lastApplied:  0,
		states:       make([]ShardData, NShards),
		gid:          gid,
		make_end:     make_end,
		ctrlers:      ctrlers,
	}
	kv.sm = shardctrler.MakeClerk(kv.ctrlers)
	for i := 0; i < shardctrler.NShards; i++ {
		kv.states[i] = *newShardState()
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	snap := kv.persister.ReadSnapshot()
	kv.applyInstallSnapshot(snap)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applier()
	go kv.nextConfigChecker()
	go kv.shardSender()
	return kv
}
