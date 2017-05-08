package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"fmt"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderIdCache int
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
	// You'll have to add code here.
	ck.leaderIdCache=0
	ck.me=nrand()
	ck.lastOpId=0
	return ck
}

func (ck *Clerk) PrintLog(format string, a ...interface{}){
	if Debug ==0{
		return
	}
	fmt.Println(fmt.Sprintf("client (%d)",ck.me)+fmt.Sprintf(format,a...))
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	leaderId:=ck.leaderIdCache
	var args GetArgs
	//var reply GetReply

	ck.lastOpId++
	args.Id=ck.lastOpId
	args.Key=key
	args.CkId=ck.me
	for{
		var reply GetReply
		leaderId=leaderId%len(ck.servers)
		ck.PrintLog("send Get request to server%d, key=%s, id=%d\n",leaderId,key,args.Id)
		ok:=ck.servers[leaderId].Call("RaftKV.Get",&args,&reply)
		if ok{
			ck.PrintLog("receive Get reply from server%d,id=%d,key=%s,value=%s,isSuc=%t\n",leaderId,args.Id,key,reply.Value,!reply.WrongLeader)
			if !reply.WrongLeader{
				ck.leaderIdCache = leaderId
				if reply.Err == OK {
					return reply.Value
				} else{
					return ""
				}
			}else{
				leaderId++
			}
		}else{
			ck.PrintLog("not receive Get from server%d,id=%d\n",leaderId,args.Id)
			leaderId++
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	leaderId:=ck.leaderIdCache
	var args PutAppendArgs

	ck.lastOpId++
	args.Id=ck.lastOpId
	args.Value=value
	args.Key=key
	args.Op=op
	args.CkId=ck.me
	for{
		var reply PutAppendReply
		leaderId=leaderId%len(ck.servers)
		ck.PrintLog("send PutAppend request to server%d, key=%s, value=%s, id=%d\n",leaderId,key,value,args.Id)
		ok:=ck.servers[leaderId].Call("RaftKV.PutAppend",&args,&reply)
		if ok{
			ck.PrintLog("receive PutAppend reply from server%d,id=%d,key=%s,value=%s,isSuc=%t\n",leaderId,args.Id,key,value,!reply.WrongLeader)
			if !reply.WrongLeader{
				ck.leaderIdCache = leaderId
				return
			}else{
				leaderId++
			}
		}else{
			ck.PrintLog("not receive PutAppend from server%d,id=%d\n",leaderId,args.Id)
			leaderId++
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
