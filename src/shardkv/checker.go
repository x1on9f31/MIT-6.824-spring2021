package shardkv

import (
	"time"

	logger "6.824/raft-logs"
)

//go routine, fetch new config when current config done
func (kv *ShardKV) nextConfigChecker() {

	for !kv.killed() {

		if has, config := kv.checkNextConfig(); has {
			kv.proposeNewConfig(config)
		}
		time.Sleep(time.Millisecond * 100)
	}
}
func (kv *ShardKV) checkNextConfig() (bool, *Config) {

	kv.mu.Lock()
	num := kv.config.Num
	kv.logger.L(logger.ServerConfig, "check new config wanted %d\n", num+1)
	kv.mu.Unlock()
	config := kv.sm.Query(num + 1)
	if config.Num == num+1 {
		kv.logger.L(logger.ServerConfig, "got new config %v old %d\n", config, num)
		return true, &config
	}
	return false, nil
}

func (kv *ShardKV) proposeNewConfig(config *Config) {
	command := &Command{
		OptType: TYPE_NEWCONFIG,
		Opt:     *config,
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if config.Num == 1+kv.config.Num {
		_, _, isLeader := kv.rf.Start(*command)
		if !isLeader {
			return
		}
		kv.logger.L(logger.ServerConfig, "propose new config %d\n", config.Num)
	}

}

func (kv *ShardKV) sendShardsToOthers() {

	for !kv.killed() {
		kv.mu.Lock()

		if kv.isCurrentConfigDone() {
			kv.logger.L(logger.ServerMove, "current config num %d done\n",
				kv.config.Num)
			kv.mu.Unlock()
			time.Sleep(time.Millisecond * 50)
			continue
		}

		m := make(map[int][]int)
		recv := make([]int, 0)
		for shardIndex, isPending := range kv.pendingShards {
			target_gid := kv.config.Shards[shardIndex]
			if isPending {
				if target_gid != kv.gid { //this shard is sending  to others
					m[target_gid] = append(m[target_gid], shardIndex)
				} else {
					recv = append(recv, shardIndex)
				}
			}
		}
		if len(m) == 0 {
			kv.logger.L(logger.ServerMove, "num %d waiting %v shards \n", kv.config.Num, recv)
			kv.mu.Unlock()
			time.Sleep(time.Millisecond * 50)
			continue
		}

		for g, shardIndexes := range m {
			kv.logger.L(logger.ServerMove, "num %d sending shards %v to group %d\n",
				kv.config.Num, shardIndexes, g)
			shardsArgs := make([]ShardIndexed, 0, len(shardIndexes))
			for _, shardIndex := range shardIndexes {
				shardsArgs = append(shardsArgs,
					ShardIndexed{
						ShardIndex: shardIndex,
						State:      *deepCopyedState(&kv.states[shardIndex]),
					})
			}
			args := &SendShardsArgs{
				Num:    kv.config.Num,
				Shards: shardsArgs,
			}

			go kv.sendToOneGroup(kv.config.Groups[g], args)
		}

		kv.mu.Unlock()
		time.Sleep(time.Millisecond * 30)
	}

}
func (kv *ShardKV) sendToOneGroup(servers []string, args *SendShardsArgs) {
	//kv.logger.L(logger.ServerMove, "num %d sending shard to group %v\n", servers)
	for _, server := range servers {
		end := kv.make_end(server)
		var reply SendShardsReply
		ok := end.Call("ShardKV.SendShards", args, &reply)
		kv.mu.Lock()
		if ok && ((kv.config.Num < reply.Num) ||
			(kv.config.Num == reply.Num && reply.Ok)) {
			kv.afterSendShardsOk(args)
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
	}
}
