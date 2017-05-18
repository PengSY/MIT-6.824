package shardkv


// import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import (
	"encoding/gob"
	"shardmaster"
	"bytes"
	"time"
	"fmt"
)

const ShardKVDebug=0
const QueryConfigInterval=0.1
const MigrationTimeout=0.5
const ReConfigureOp="ReConfig"
const MigrationOp="Migration"
const(
	MigrationOk="MigrateOK"
	StaleConfig="StaleConfig"
)


func (kv *ShardKV) PrintLog(format string, a ...interface{}){
	if ShardKVDebug==0 || kv.isDead{
		return
	}
	fmt.Println(fmt.Sprintf("gid:%d,s%d:",kv.gid,kv.me)+fmt.Sprintf(format,a...))
}

type ReConfigureArgs struct {
	Config shardmaster.Config
}

type ShardMigrationArgs struct {
	ConfigNum int
	MyShard Shard
}

type ShardMigrationReply struct {
	WrongLeader bool
	//Err string
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	MyType OpType
	Args interface{}
	replyCh interface{}
	LeaderId int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	LatestCommitId map[int64]int
	isDead bool
	mck *shardmaster.Clerk
	Config shardmaster.Config
	Shards map[int]Shard
	persister *raft.Persister
	ConfigComplete bool
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.mu.Lock()
	kv.isDead=true
	kv.mu.Unlock()
}

func (kv *ShardKV) ConfigMonitor(){
	for{
		time.Sleep(QueryConfigInterval*1000*time.Millisecond)
		kv.mu.Lock()
		if kv.isDead{
			kv.mu.Unlock()
			return
		}
		if _,isLeader:=kv.rf.GetState();!isLeader || !kv.ConfigComplete{
			continue
		}
		newConfig:=kv.mck.Query(kv.Config.Num+1)
		if newConfig.Num>kv.Config.Num{
			var op Op
			op.MyType=ReConfigureOp
			op.Args=ReConfigureArgs{newConfig}
			if _,_,isLeader:=kv.rf.Start(op);!isLeader{
				continue
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) ShardMigrationRPC(args *ShardMigrationArgs,reply *ShardMigrationReply){
	var op Op
	var replyCh chan ShardMigrationReply

	replyCh=make(chan ShardMigrationReply)
	op.Args=*args
	op.LeaderId=kv.me
	op.MyType=MigrationOp

	if _,_,isLeader:=kv.rf.Start(op);!isLeader{
		reply.WrongLeader=true
		return
	}
	timer:=time.NewTimer(MigrationTimeout*1000*time.Millisecond)
	select{
	case <-timer.C:
		reply.WrongLeader=true
	case *reply<-replyCh:
	}
	return
}

func (kv *ShardKV) IsShardMigrationComplete() bool{
	totalShards:=0
	for shardId,gid:=range kv.Config.Shards{
		if gid!=kv.gid{
			continue
		}
		if _,ok:=kv.Shards[shardId];!ok{
			return false
		}
		totalShards++
	}
	if len(kv.Shards)!=totalShards{
		return false
	}
	return true
}

func (kv *ShardKV) RunShardMigration(op Op){
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var replyCh chan ShardMigrationReply
	var reply ShardMigrationReply
	args:=op.Args.(ShardMigrationArgs)

	if op.LeaderId==kv.me && op.replyCh!=nil{
		replyCh=op.replyCh.(chan ShardMigrationReply)
	}

	if args.ConfigNum<kv.Config.Num && replyCh!=nil{
		if replyCh!=nil{
			reply.WrongLeader = false
			//reply.Err = StaleConfig
			replyCh <- reply
		}
		return
	}else if args.ConfigNum>kv.Config.Num{
		if replyCh!=nil{
			reply.WrongLeader=true
			replyCh<-reply
		}
		return
	}

	if _,ok:=kv.Shards[args.MyShard.Id];ok{
		if replyCh!=nil{
			reply.WrongLeader=false
			//reply.Err=MigrationOk
			replyCh<-reply
		}
		return
	}

	if kv.Config.Shards[args.MyShard.Id]!=kv.gid{
		kv.PrintLog("Give me wrong shard,shardId=%d,should belong to group %d",args.MyShard.Id,kv.Config.Shards[args.MyShard.Id])
	}

	kv.Shards[args.MyShard.Id]=args.MyShard
	if replyCh!=nil{
		reply.WrongLeader=false
		//reply.Err=MigrationOk
		replyCh<-reply
	}
	kv.ConfigComplete=kv.IsShardMigrationComplete()
}

func (kv *ShardKV) SendShardMigrationRPC(servers []string,args *ShardMigrationArgs){
	shardId:=args.MyShard.Id
	Loop:
	for{
		for server:=range servers{
			s:=kv.make_end(server)
			var reply ShardMigrationReply

			ok:=s.Call("ShardKV.ShardMigrationRPC",args,&reply)
			if ok && !reply.WrongLeader{
				break Loop
			}
		}
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.Shards,shardId)
	kv.ConfigComplete=kv.IsShardMigrationComplete()
}

func (kv *ShardKV) RunReconfigure(op Op){
	kv.mu.Lock()
	defer kv.mu.Unlock()

	newConfig:=op.Args.(ReConfigureArgs).Config
	if newConfig.Num<=kv.Config{
		return
	}
	kv.Config=newConfig
	for shardId,shard:=range kv.Shards{
		if kv.Config.Shards[shardId]!=kv.gid{
			var args ShardMigrationArgs

			args.ConfigNum=newConfig.Num
			args.MyShard=shard
			go kv.SendShardMigrationRPC(newConfig.Groups[newConfig.Shards[shardId]],&args)
		}
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(ReConfigureArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.isDead=false
	kv.LatestCommitId=make(map[int64]int)
	kv.mck=shardmaster.MakeClerk(kv.masters)
	kv.Shards=make(map[int]Shard)
	kv.persister=persister
	kv.ConfigComplete=true

	reader:=bytes.NewBuffer(kv.persister.ReadSnapshot())
	decoder:=gob.NewDecoder(reader)
	decoder.Decode(kv)

	return kv
}
