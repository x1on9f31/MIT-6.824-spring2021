package shardkv

import (
	"time"

	logger "6.824/raft-logs"
)

//go routine, fetch new config when current config done
func (kv *ShardKV) nextConfigChecker() {

	for !kv.killed() {

		kv.mu.Lock()
		num := kv.config.Num
		kv.logger.L(logger.ShardKVConfig, "check new config wanted %d\n", num+1)
		kv.mu.Unlock()

		if config := kv.sm.Query(num + 1); config.Num == num+1 {
			kv.logger.L(logger.ShardKVConfig, "got new config num %d, %v\n", num+1, config.Shards)
			kv.proposeNewConfig(&config)
		}

		time.Sleep(time.Millisecond * 100)
	}
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
		kv.logger.L(logger.ShardKVConfig, "propose new config %d\n", config.Num)
	}

}
