package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "labrpc"
import "crypto/rand"
import "math/big"
import "shardmaster"
import (
	"time"
	"fmt"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	lastOpId int
	me int64
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.lastOpId=0
	ck.me=nrand()
	return ck
}

func (ck *Clerk) PrintLog(format string, a ...interface{}){
	if ShardKVDebug==0{
		return
	}
	fmt.Println(fmt.Sprintf("c%d:",ck.me)+fmt.Sprintf(format,a...))
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	shard := key2shard(key)
	args.ShardId=shard

	for {
		args.ConfigNum=ck.config.Num
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ck.PrintLog("send Get request to gid %d, key=%s, shardId=%d,configNum=%d",gid,args.Key,args.ShardId,ck.config.Num)
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.PrintLog("receive success Get reply from gid %d, shardId=%d",gid,args.ShardId)
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					ck.PrintLog("receive Get reply from gid %d ,key=%s, shardId=%d, WrongGroup",gid,args.Key,args.ShardId)
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	shard := key2shard(key)
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	ck.lastOpId++
	args.OpId=ck.lastOpId
	args.CkId=ck.me
	args.ShardId=shard

	for {
		args.ConfigNum=ck.config.Num
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ck.PrintLog("send PutAppend request to gid %d, key=%s, value=%s, shardId=%d, opid=%d,configNum=%d",gid,args.Key,args.Value,args.ShardId,args.OpId,ck.config.Num)
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.WrongLeader == false && reply.Err == OK {
					ck.PrintLog("recieve success PutAppend request from gid %d, key=%s, value=%s, shardId=%d",gid,args.Key,args.Value,args.ShardId)
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					ck.PrintLog("receive PutAppend  request from gid %d, key=%s, value=%s, shardId=%d, WrongGroup",gid,args.Key,args.Value,args.ShardId)
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
