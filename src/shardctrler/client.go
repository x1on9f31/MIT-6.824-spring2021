package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

var used_ID map[int64]bool
var map_lock sync.Mutex

func init() {
	used_ID = make(map[int64]bool)
	used_ID[-1] = true
}

func getUnusedClientID() int64 {
	map_lock.Lock()
	defer map_lock.Unlock()
	for {
		id := nrand()
		if !used_ID[id] {
			used_ID[id] = true
			return id
		}
	}
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	id        int64
	serverCnt int
	seq       int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.serverCnt = len(servers)
	ck.id = getUnusedClientID()
	ck.seq = 0
	// Your code here.
	return ck
}

//todo 增加超时机制
func (ck *Clerk) Query(num int) Config {

	for {
		// Your code here.
		// try each known server.
		for _, srv := range ck.servers {
			args := &QueryArgs{
				ClientID: ck.id,
				Seq:      ck.seq,
				Num:      num,
			}
			timer := time.NewTimer(time.Millisecond * 100)
			done := make(chan *QueryReply)
			go func(d chan *QueryReply) {
				var reply QueryReply
				ok := srv.Call("ShardCtrler.Query", args, &reply)
				if !ok {
					reply.WrongLeader = true
				}
				d <- &reply
			}(done)

			select {
			case <-timer.C:
			case reply := <-done:
				if !reply.WrongLeader {
					ck.seq++
					return reply.Config
				}
			}
		}
	}
}

func (ck *Clerk) Join(servers map[int][]string) {

	// Your code here.

	for {

		// try each known server.

		for _, srv := range ck.servers {
			args := &JoinArgs{
				ClientID: ck.id,
				Seq:      ck.seq,
				Servers:  servers,
			}
			timer := time.NewTimer(time.Millisecond * 100)
			done := make(chan *JoinReply)

			go func(d chan *JoinReply) {
				var reply JoinReply
				ok := srv.Call("ShardCtrler.Join", args, &reply)
				if !ok {
					reply.WrongLeader = true
				}
				d <- &reply
			}(done)

			select {
			case <-timer.C:
			case reply := <-done:
				if !reply.WrongLeader {
					ck.seq++
					return
				}
			}
		}
	}
}

func (ck *Clerk) Leave(gids []int) {

	for {

		// Your code here.

		// try each known server.
		for _, srv := range ck.servers {
			args := &LeaveArgs{
				ClientID: ck.id,
				Seq:      ck.seq,
				GIDs:     gids,
			}
			timer := time.NewTimer(time.Millisecond * 100)
			done := make(chan *LeaveReply)

			go func(d chan *LeaveReply) {
				var reply LeaveReply
				ok := srv.Call("ShardCtrler.Leave", args, &reply)
				if !ok {
					reply.WrongLeader = true
				}
				d <- &reply
			}(done)

			select {
			case <-timer.C:
			case reply := <-done:
				if !reply.WrongLeader {
					ck.seq++
					return
				}
			}
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {

	// Your code here.

	for {
		// try each known server.
		for _, srv := range ck.servers {
			args := &MoveArgs{
				Shard:    shard,
				GID:      gid,
				ClientID: ck.id,
				Seq:      ck.seq,
			}
			timer := time.NewTimer(time.Millisecond * 100)
			done := make(chan *MoveReply)

			go func(d chan *MoveReply) {
				var reply MoveReply
				ok := srv.Call("ShardCtrler.Move", args, &reply)
				if !ok {
					reply.WrongLeader = true
				}
				d <- &reply
			}(done)

			select {
			case <-timer.C:
			case reply := <-done:
				if !reply.WrongLeader {
					ck.seq++
					return
				}
			}
		}
	}
}
