package shardctrler

import (
	logger "6.824/raft-logs"
)

func (sc *ShardCtrler) getBalancedShards(cnt_map map[int]int, newShards [NShards]int) [NShards]int {
	groupCnt := len(cnt_map)
	if groupCnt == 0 {
		for i := 0; i < NShards; i++ {
			newShards[i] = 0
		}
		return newShards
	}

	avg := NShards / groupCnt
	extra := NShards % groupCnt
	//get optimized assignment
	optimized_assign := make([]int, groupCnt)
	for i, _ := range optimized_assign {
		optimized_assign[i] = avg
	}
	for i := groupCnt - 1; i >= 0 && extra > 0; i-- {
		optimized_assign[i]++
		extra--
	}

	index_arr := make([]int, 0, groupCnt)
	for g, _ := range cnt_map {
		index_arr = append(index_arr, g)
	}
	//bubble sort, first by number of owned shards, then by gid
	for i := 0; i < groupCnt-1; i++ {
		for j := groupCnt - 1; j > i; j-- {
			if cnt_map[index_arr[j]] < cnt_map[index_arr[j-1]] ||
				(cnt_map[index_arr[j]] == cnt_map[index_arr[j-1]] &&
					index_arr[j] < index_arr[j-1]) {
				t := index_arr[j]
				index_arr[j] = index_arr[j-1]
				index_arr[j-1] = t
			}
		}
	}

	sc.logger.L(logger.CtrlerBalance, "sorted map:%v\n", index_arr)

	//let some groups release shards util reach their optimized assignment
	for i := groupCnt - 1; i >= 0; i-- {
		if optimized_assign[i] < cnt_map[index_arr[i]] {
			free_g := index_arr[i]
			should_free := cnt_map[free_g] - optimized_assign[i]
			for j, g := range newShards {
				if should_free <= 0 {
					break
				}
				if g == free_g {
					newShards[j] = 0
					should_free--
				}
			}
			cnt_map[free_g] = optimized_assign[i]
		}
	}
	//assign the free shards to other groups
	for i := 0; i < groupCnt; i++ {
		if optimized_assign[i] > cnt_map[index_arr[i]] {
			fill_g := index_arr[i]
			should_fill := optimized_assign[i] - cnt_map[fill_g]
			for j, g := range newShards {
				if should_fill <= 0 {
					break
				}
				if g == 0 {
					newShards[j] = fill_g
					should_fill--
				}
			}
			cnt_map[fill_g] = optimized_assign[i]
		}

	}

	sc.logger.L(logger.CtrlerBalance, "preAssign %d after balance %v\n", optimized_assign, newShards)
	return newShards
}

func (sc *ShardCtrler) doJoin(servers map[int][]string) *Config {

	old := &sc.configs[len(sc.configs)-1]
	sc.logger.L(logger.CtrlerJoin, "join %v old %v\n", servers, old)

	new_group_map := joinServers(servers, old.Groups)

	cnt_map := make(map[int]int)
	for g := range new_group_map {
		cnt_map[g] = 0
	}
	for _, g := range old.Shards {
		if g != 0 {
			cnt_map[g]++
		}

	}

	sc.logger.L(logger.CtrlerJoin, "cnt_map :%v\n", cnt_map)
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.getBalancedShards(cnt_map, old.Shards),
		Groups: new_group_map,
	}
}
func (sc *ShardCtrler) doLeave(gids []int) *Config {

	old := &sc.configs[len(sc.configs)-1]
	sc.logger.L(logger.CtrlerLeave, "leave %v old %v\n", gids, old)
	leave_gids := make(map[int]bool)
	for _, v := range gids {
		leave_gids[v] = true
	}

	new_group_map := leaveServers(leave_gids, old.Groups)

	new_shard := old.Shards
	cnt_map := make(map[int]int)
	for g := range new_group_map {
		if !leave_gids[g] {
			cnt_map[g] = 0
		}

	}
	for i, g := range old.Shards {
		if g != 0 {
			if leave_gids[g] {
				new_shard[i] = 0
			} else {
				cnt_map[g]++
			}
		}

	}
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.getBalancedShards(cnt_map, new_shard),
		Groups: new_group_map,
	}
}

func (sc *ShardCtrler) doMove(args GIDandShard) *Config {
	old := &sc.configs[len(sc.configs)-1]
	new_shards := old.Shards
	new_shards[args.Shard] = args.GID
	sc.logger.L(logger.CtrlerMove, "move shards %v\n", args)
	return &Config{
		Num:    len(sc.configs),
		Shards: new_shards,
		Groups: cloneServers(old.Groups),
	}
}

func (sc *ShardCtrler) queryConfig(num int) *Config {
	if num < 0 {
		length := len(sc.configs)
		sc.logger.L(logger.CtrlerQuery, "query %d current %#v\n", num, sc.configs[len(sc.configs)-1])
		return &sc.configs[length-1]
	}
	if num == -1 || num >= len(sc.configs)-1 {
		return &sc.configs[len(sc.configs)-1]
	}
	return &sc.configs[num]
}
