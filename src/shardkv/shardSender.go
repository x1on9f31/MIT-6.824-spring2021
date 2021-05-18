package shardkv

import (
	"time"

	logger "6.824/raft-logs"
)

func (kv *ShardKV) shardSender() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	timer := time.NewTimer(time.Millisecond * 100)
	done := make(chan bool)
	go func() {
		for !kv.killed() {
			select {
			case <-timer.C:
				kv.senderCond.Signal()
				timer.Reset(time.Millisecond * 100)
			case <-done:
				return
			}
		}
	}()

	for !kv.killed() {

		if kv.isCurrentConfigDone() {
			kv.logger.L(logger.ShardKVMigration, "current config num %d done\n",
				kv.config.Num)
			kv.senderCond.Wait()
			continue
		}

		to_send, to_recv := kv.getSendAndRecvTarget()

		if len(to_send) == 0 {
			kv.logger.L(logger.ShardKVMigration, "num %d waiting %v shards \n", kv.config.Num, to_recv)
			kv.senderCond.Wait()
			continue
		}

		for g, shardIndexes := range to_send {
			kv.logger.L(logger.ShardKVMigration, "num %d sending shards %v to group %d\n",
				kv.config.Num, shardIndexes, g)

			shardDatas := make([]ShardData, 0, len(shardIndexes))
			for _, index := range shardIndexes {
				shardDatas = append(shardDatas, *deepCopyedState(&kv.states[index]))
			}
			args := &MigrationArgs{
				Num:           kv.config.Num,
				ShardDatas:    shardDatas,
				ShardsIndexes: shardIndexes,
			}

			go kv.sendToGroup(kv.config.Groups[g], args)
		}

		kv.senderCond.Wait()
	}
	close(done)
}
func (kv *ShardKV) sendToGroup(servers []string, args *MigrationArgs) {

	for _, server := range servers {
		end := kv.make_end(server)
		var reply MigrationReply
		ok := end.Call("ShardKV.Migrate", args, &reply)
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

func (kv *ShardKV) afterSendShardsOk(args *MigrationArgs) {
	kv.logger.L(logger.ShardKVApply, "after send %v shards ok\n", args.ShardsIndexes)
	if kv.config.Num != args.Num {
		return
	}
	if kv.isMigrationDone(args) {
		return
	}
	command := &Command{
		OptType: TYPE_MIGRATE,
		Opt:     *args,
	}
	_, _, isLeader := kv.rf.Start(*command)

	if !isLeader {
		return
	} else {
		kv.logger.L(logger.ShardKVMigration,
			"propose migration after send %v shards ok, num %d\n", args.ShardsIndexes, args.Num)
	}

}

func (kv *ShardKV) getSendAndRecvTarget() (map[int][]int, []int) {
	send := make(map[int][]int)
	recv := make([]int, 0)
	for shardIndex, isPending := range kv.pendingShards {
		target_gid := kv.config.Shards[shardIndex]
		if isPending {
			if target_gid != kv.gid { //this shard is sending  to others
				send[target_gid] = append(send[target_gid], shardIndex)
			} else {
				recv = append(recv, shardIndex)
			}
		}
	}
	return send, recv
}
