package shardkv

import (
	"bytes"
	"strconv"

	"6.824/labgob"
	logger "6.824/raft-logs"
)

//done inside apply
func (kv *ShardKV) applyConfig(newConfig *Config) {
	if newConfig.Num <= kv.config.Num {
		return
	}

	if newConfig.Num > kv.config.Num+1 {
		panic("config num gap")
	}
	if !kv.isCurrentConfigDone() {
		kv.logger.L(logger.ServerConfig, "apply new config num %d failed, for old num %d still pending:%v\n",
			newConfig.Num, kv.config.Num,
			kv.pendingShards)
		return
	}

	kv.logger.L(logger.ServerConfig, "apply new config num %d :%v\n old :%v\n", newConfig.Num, newConfig,
		kv.config)
	kv.initPending(newConfig)

	kv.config = *newConfig
	recv := make([]int, 0)
	send := make([]int, 0)
	for i, pending := range kv.pendingShards {
		if pending {
			if kv.config.Shards[i] == kv.gid {
				recv = append(recv, i)
			} else {
				send = append(send, i)
			}
		}
	}
	kv.logger.L(logger.ServerConfig, "inited need send %v,need recv %v\n", send, recv)
	//kv.logger.L(logger.ServerApply, "new config is %v\n", kv.config)
}
func (kv *ShardKV) applyMigrate(args *MigrateCommand) {
	num := args.Num
	if kv.config.Num > num {
		return
	}
	if kv.config.Num < num {
		panic("apply migration from higher config")
	}
	mig_shards := make([]int, 0)
	for _, shardsI := range args.Shards {
		mig_shards = append(mig_shards, shardsI.ShardIndex)
	}
	kv.logger.L(logger.ServerMove, "apply num %d migration %v\n", kv.config.Num, mig_shards)
	for _, shardIndexed := range args.Shards {
		kv.decidedShard(&shardIndexed)
	}

}
func (kv *ShardKV) afterSendShardsOk(args *SendShardsArgs) {
	kv.logger.L(logger.ServerApply, "after send shards ok\n")
	if kv.config.Num != args.Num {
		return
	}
	mig_shards := make([]int, 0)
	for _, shardsI := range args.Shards {
		mig_shards = append(mig_shards, shardsI.ShardIndex)
	}
	if kv.isDone(args) {
		return
	}
	command := &Command{
		OptType: TYPE_MIGRATE,
		Opt:     *args,
	}
	_, _, isLeader := kv.rf.Start(*command)

	if !isLeader {
		kv.logger.L(logger.ServerMove, "move delete propose not leader\n")
		return
	} else {
		kv.logger.L(logger.ServerMove, "move delete propose num %d shards %v as leader?\n", args.Num, mig_shards)
	}

}

func (kv *ShardKV) initPending(newConfig *Config) {
	for i := 0; i < NShards; i++ {
		//need transfer shards by me
		if (kv.config.Shards[i] == kv.gid) != (newConfig.Shards[i] == kv.gid) {
			//0 -> me ,empty shard assign to me, need not pending
			if kv.config.Shards[i] == 0 {
				continue
			}

			//me -> 0, delete shards imediately, need not pending
			if newConfig.Shards[i] == 0 {
				kv.states[i] = *newShardState()
				continue
			}
			kv.pendingShards[i] = true

		}
	}
}

func (kv *ShardKV) decidedShard(shardIndexed *ShardIndexed) {
	shardIndex := shardIndexed.ShardIndex
	if kv.pendingShards[shardIndex] { //ignore duplicated apply
		kv.pendingShards[shardIndex] = false
		if kv.config.Shards[shardIndex] == kv.gid { //recv
			kv.logger.L(logger.ServerApply, "safely recv and decided shard %d\n", shardIndex)
			deepCopyState(&kv.states[shardIndex], &shardIndexed.State) //update state, map from raft, should deep copy
		} else { //delete ,send over
			kv.states[shardIndex] = *newShardState()
			kv.logger.L(logger.ServerApply, "safely delete shard %d\n", shardIndex)
		}
	}
}
func (kv *ShardKV) notify(index int) {
	if c, ok := kv.reply_chan[index]; ok {
		close(c)
		delete(kv.reply_chan, index)
	}
}

//recv ApplyMsg from applyCh
func (kv *ShardKV) applier() {
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

			if v, ok := m.Command.(Command); !ok {
				//err
				kv.logger.L(logger.ServerApply, "nop apply %#v\n", m.Command)
				//panic("not ok assertion in apply!")
			} else {
				kv.logger.L(logger.ServerApply, "apply index %d key%v num %d type %d   lastApplied %d\n", m.CommandIndex,
					v.Key, v.Num, v.OptType, kv.lastApplied)
				kv.applyCommand(v) //may ignore duplicate cmd
				//kv.logger.L(logger.ServerApply, "after apply %s\n", kv.printKV())
			}
			kv.lastApplied = m.CommandIndex
			if kv.needSnapshot() {
				kv.doSnapshotForRaft(m.CommandIndex)
				kv.logger.L(logger.ServerSnapSize, "after snap shot size%d %d\n",
					kv.persister.RaftStateSize(), kv.persister.SnapshotSize())
			}
			kv.notify(m.CommandIndex)

		} else if m.CommandValid && m.CommandIndex != 1+kv.lastApplied {
			// out of order cmd, just ignore
			kv.logger.L(logger.ServerApply, "ignore apply %v for lastApplied %v\n",
				m.CommandIndex, kv.lastApplied)
		} else {
			kv.logger.L(logger.ServerApply, "Wrong apply msg\n")
		}

		kv.mu.Unlock()
	}

}
func (kv *ShardKV) printKV() string {
	res := "["
	for i := 0; i < NShards; i++ {
		res += strconv.Itoa(i) + "{"
		for k, _ := range kv.states[i].KVmap {
			res += k + ","
		}
		res += "}"
	}
	res += "]"
	return res
}
func (kv *ShardKV) applyCommand(v Command) {

	switch v.OptType {
	case TYPE_NEWCONFIG:
		config := v.Opt.(Config)
		kv.applyConfig(&config)
	case TYPE_MIGRATE:
		migrate := v.Opt.(MigrateCommand)
		kv.applyMigrate(&migrate)

	default:
		key := v.Key
		shardIndex := key2shard(key)
		if !kv.isResponsible(shardIndex, v.Num) || !kv.isAvaiable(shardIndex) {
			return
		}
		if kv.states[shardIndex].NextSeq[v.ClientID] > v.Seq {
			return
		}

		kv.states[shardIndex].NextSeq[v.ClientID] = v.Seq + 1

		if v.OptType == TYPE_PUT || v.OptType == TYPE_APPEND {
			value := v.Opt.(string)
			if v.OptType == TYPE_PUT {
				kv.states[shardIndex].KVmap[key] = value
				//kv.logger.L(logger.ServerApply, "put %v to shard %d \n", key, shardIndex)
			} else if v.OptType == TYPE_APPEND {
				kv.states[shardIndex].KVmap[key] += value
				//kv.logger.L(logger.ServerApply, "append %v to shard %d %v\n", key, shardIndex, kv.states[shardIndex].KVmap[key])
			}

		}
	}

}

//hold lock
func (kv *ShardKV) applyInstallSnapshot(snap []byte) {
	if snap == nil || len(snap) < 1 { // bootstrap without any state?
		kv.logger.L(logger.ServerSnap, "empty snap\n")
		return
	}

	r := bytes.NewBuffer(snap)
	d := labgob.NewDecoder(r)

	lastIndex := 0
	var pendingShards [NShards]bool
	var config Config
	var states []ShardState

	if d.Decode(&lastIndex) != nil ||
		d.Decode(&pendingShards) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&states) != nil {
		kv.logger.L(logger.ServerSnap, "apply install decode err\n")
		panic("err decode snap")
	} else {
		kv.lastApplied = lastIndex
		kv.pendingShards = pendingShards
		kv.config = config
		kv.states = states
		kv.logger.L(logger.ServerApply, "install snap index %d,config %v\n pending shards %v\n",
			lastIndex, config, pendingShards)
	}

}

//hold lock
func (kv *ShardKV) doSnapshotForRaft(index int) {

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// v := m.Command
	// e.Encode(v)
	lastIndex := kv.lastApplied
	e.Encode(lastIndex)
	e.Encode(kv.pendingShards)
	e.Encode(kv.config)
	e.Encode(kv.states)
	snap := w.Bytes()
	kv.logger.L(logger.ServerSnap, "do snapshot for raft %v %v,size %d\n",
		index, kv.lastApplied, len(snap))

	kv.rf.Snapshot(index, snap)
}

//hold lock
func (kv *ShardKV) needSnapshot() bool {
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
