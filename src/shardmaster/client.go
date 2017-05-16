package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import (
	"math/big"
	"fmt"
)

func (ck *Clerk) PrintLog(format string, a ...interface{}){
	if ShardMasterDebug==0{
		return
	}
	fmt.Println(fmt.Sprintf("c%d:",ck.me)+fmt.Sprintf(format,a...))
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	me int64
	lastOpId int
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
	// Your code here.
	ck.me=nrand()
	ck.lastOpId=0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	ck.lastOpId++
	args.OpId=ck.lastOpId
	args.CkId=ck.me
	args.Num = num

	ck.PrintLog("send Query request,opid=%d",args.OpId)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	ck.lastOpId++
	args.OpId=ck.lastOpId
	args.CkId=ck.me
	args.Servers = servers

	ck.PrintLog("send Join request,opid=%d,new servers=%v",args.OpId,args.Servers)
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	ck.lastOpId++
	args.OpId=ck.lastOpId
	args.CkId=ck.me
	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	ck.lastOpId++
	args.OpId=ck.lastOpId
	args.CkId=ck.me
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
