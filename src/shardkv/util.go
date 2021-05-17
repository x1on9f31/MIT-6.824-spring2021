package shardkv

//todo
func (kv *ShardKV) isResponsible(shard, num int) bool {
	return num == kv.config.Num && kv.config.Shards[shard] == kv.gid
}
func (kv *ShardKV) isAvaiable(shard int) bool {
	return !kv.pendingShards[shard]
}

func (kv *ShardKV) isCurrentConfigDone() bool {
	for i := 0; i < NShards; i++ {
		if kv.pendingShards[i] {
			return false
		}
	}
	return true
}

func newShardState() *ShardData {
	return &ShardData{
		KVmap:   make(map[string]string),
		NextSeq: make(map[int64]int),
	}
}

func deepCopyState(dst *ShardData, from *ShardData) {
	*dst = *deepCopyedState(from)
}
func deepCopyedState(from *ShardData) *ShardData {
	dst := newShardState()
	for k, v := range from.KVmap {
		dst.KVmap[k] = v
	}
	for k, v := range from.NextSeq {
		dst.NextSeq[k] = v
	}
	return dst
}
