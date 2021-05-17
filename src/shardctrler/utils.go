package shardctrler

import (
	"fmt"
	"testing"
)

func joinServers(gids, groups map[int][]string) map[int][]string {
	res := make(map[int][]string)
	for k, v := range groups {
		res[k] = v
	}
	for k, v := range gids {
		res[k] = v
	}
	return res
}
func leaveServers(leaveGIDs map[int]bool, groups map[int][]string) map[int][]string {
	res := make(map[int][]string)

	for k, v := range groups {
		if !leaveGIDs[k] {
			res[k] = v
		}

	}
	return res
}
func cloneServers(groups map[int][]string) map[int][]string {
	res := make(map[int][]string)
	for k, v := range groups {
		res[k] = v
	}
	return res
}

func abs(i float64) float64 {
	if i >= 0 {
		return i
	}
	return -i
}

func testBalance(t *testing.T) {
	oldShards := [9]int{0, 0, 0, 0, 2, 0, 0, 1, 0}
	cnt_map := make(map[int]int)
	avg := len(oldShards) / 3
	for _, g := range oldShards {
		cnt_map[g]++
	}

	left := make([]int, 0, 10)
	new_shards := oldShards

	for i, g := range oldShards {
		if cnt_map[g] > avg+1 {
			left = append(left, i)
			cnt_map[g]--
		}
	}
	for _, g := range oldShards {
		if len(left) <= 0 {
			break
		}
		if cnt_map[g] < avg {
			for cnt_map[g] < avg && len(left) > 0 {
				cnt_map[g]++
				new_shards[left[len(left)-1]] = g
				left = left[:len(left)-1]
			}

		}
	}
	fmt.Println(new_shards)
	t.Error(new_shards)
}
