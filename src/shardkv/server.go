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
	"log"
)

const ShardKVDebug=0
const QueryConfigInterval=0.1
const GarbageCollectorInterval=0.1
const MigrationTimeout=0.5
const ClientResendTimeout=0.5

const ReConfigureOp="ReConfig"
const MigrationOp="Migration"
const DeleteGarbageShardOp="DeleteGarbageShard"

func (kv *ShardKV) PrintLog(format string, a ...interface{}){
	if ShardKVDebug==0 || kv.isDead{
		return
	}
	fmt.Println(fmt.Sprintf("config%d, gid:%d, s%d:",kv.Config.Num,kv.gid,kv.me)+fmt.Sprintf(format,a...))
}

type ReConfigureArgs struct {
	Config shardmaster.Config
}

type DeleteGarbageShardArgs struct {
	ShardId int
}

type ShardMigrationArgs struct {
	ConfigNum int
	MyShard Shard
	LatestCommitId map[int64]int
}

type ShardMigrationReply struct {
	WrongLeader bool
}

type Garbage struct {
	//Gid int
	//GarbageId int
	Servers []string
	Shard Shard
	ConfigNum int
	LatestCommitId map[int64]int
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
	isDead bool
	mck *shardmaster.Clerk
	persister *raft.Persister

	LatestCommitId map[int64]int
	Config shardmaster.Config
	Shards map[int]Shard
	ConfigComplete bool
	Garbages map[int]Garbage
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var op Op
	var replyCh chan GetReply

	replyCh=make(chan GetReply)
	op.Args=*args
	op.MyType=GetOp
	op.LeaderId=kv.me
	op.replyCh=replyCh

	kv.PrintLog("receive Get request, key=%s",args.Key)
	if _,_,isLeader:=kv.rf.Start(op);!isLeader{
		reply.WrongLeader=true
		return
	}
	kv.PrintLog("waiting Get request to reach agreement, key=%s",args.Key)

	timer:=time.NewTimer(ClientResendTimeout*1000*time.Millisecond)
	select {
	case <-timer.C:
		reply.WrongLeader=true
	case *reply=<-replyCh:
		kv.PrintLog("return Get request, key=%s",args.Key)
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var op Op
	var replyCh chan PutAppendReply

	replyCh=make(chan PutAppendReply)
	op.Args=*args
	op.MyType=OpType(args.Op)
	op.LeaderId=kv.me
	op.replyCh=replyCh

	kv.PrintLog("receive PutAppend request from c%d, key=%s",args.CkId,args.Key)
	if _,_,isLeader:=kv.rf.Start(op);!isLeader{
		reply.WrongLeader=true
		return
	}
	kv.PrintLog("waiting PutAppend request to reach agreement, key=%s",args.Key)

	timer:=time.NewTimer(ClientResendTimeout*1000*time.Millisecond)
	select {
	case <-timer.C:
		reply.WrongLeader=true
	case *reply=<-replyCh:
		kv.PrintLog("return PutAppend request to c%d, key=%s",args.CkId,args.Key)
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	// Your code here, if desired.
	kv.PrintLog("I am going to dead")
	//kv.mu.Lock()
	kv.PrintLog("kill call raft kill")
	kv.rf.Kill()
	kv.PrintLog("I am dead")
	kv.isDead=true
	//kv.mu.Unlock()
}

func (kv *ShardKV) ConfigMonitor(){
	for{
		time.Sleep(QueryConfigInterval*1000*time.Millisecond)
		var oldConfigNum int

		kv.mu.Lock()
		//kv.PrintLog("config monitor get kv lock")
		if kv.isDead{
			kv.mu.Unlock()
			//kv.PrintLog("config monitor release kv lock")
			return
		}
		kv.ConfigComplete=kv.IsConfigComplete()
		if !kv.ConfigComplete{
			kv.mu.Unlock()
			kv.PrintLog("not query new config, my last config don't complete")
			//time.Sleep(GarbageCollectorInterval*1000*time.Millisecond)
			continue
		}
		oldConfigNum=kv.Config.Num
		kv.mu.Unlock()
		//kv.PrintLog("config monitor call raft getstate")
		if _,isLeader:=kv.rf.GetState();!isLeader{
			//kv.mu.Unlock()
			//kv.PrintLog("config monitor release kv lock")
			continue
		}

		newConfig:=kv.mck.Query(kv.Config.Num+1)
		kv.PrintLog("Get new config %d %v, my config Num:%d",newConfig.Num,newConfig.Shards,kv.Config.Num)
		if newConfig.Num>oldConfigNum{
			var op Op
			op.MyType=ReConfigureOp
			op.Args=ReConfigureArgs{newConfig}
			//kv.PrintLog("config monitor call raft start")
			if _,_,isLeader:=kv.rf.Start(op);!isLeader{
				//kv.mu.Unlock()
				//kv.PrintLog("config monitor release kv lock")
				continue
			}
		}
		//kv.mu.Unlock()
		//kv.PrintLog("config monitor release kv lock")
	}
}

func (kv *ShardKV) ShardMigrationRPC(args *ShardMigrationArgs,reply *ShardMigrationReply){
	var op Op
	var replyCh chan ShardMigrationReply

	replyCh=make(chan ShardMigrationReply)
	op.Args=*args
	op.LeaderId=kv.me
	op.MyType=MigrationOp
	op.replyCh=replyCh

	if _,_,isLeader:=kv.rf.Start(op);!isLeader{
		reply.WrongLeader=true
		return
	}
	kv.PrintLog("receive shard migration rpc, shardId=%d",args.MyShard.Id)
	timer:=time.NewTimer(MigrationTimeout*1000*time.Millisecond)
	select{
	case <-timer.C:
		reply.WrongLeader=true
	case *reply=<-replyCh:
	}
	return
}

func (kv *ShardKV) IsConfigComplete() bool{
	isComplete:=true
	for shardId,gid:=range kv.Config.Shards{
		if gid==kv.gid{
			if _,ok:=kv.Shards[shardId];!ok{
				kv.PrintLog("config %d don't complete, need shard %d",kv.Config.Num,shardId)
				isComplete=false
			}
		}
	}
	if isComplete{
		kv.PrintLog("config %d complete",kv.Config.Num)
	}
	return isComplete
}

func (kv *ShardKV) RunShardMigration(op Op){
	kv.mu.Lock()
	//defer kv.PrintLog("quit run shard migration")
	defer kv.mu.Unlock()

	//kv.PrintLog("in run shard migration")
	var replyCh chan ShardMigrationReply
	var reply ShardMigrationReply
	var shard Shard
	args:=op.Args.(ShardMigrationArgs)
	CopyShard(&shard,&args.MyShard)

	if op.LeaderId==kv.me && op.replyCh!=nil{
		replyCh=op.replyCh.(chan ShardMigrationReply)
	}

	if args.ConfigNum<kv.Config.Num{
		if replyCh!=nil{
			reply.WrongLeader = false
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

	if _,ok:=kv.Shards[shard.Id];ok{
		if replyCh!=nil{
			reply.WrongLeader=false
			replyCh<-reply
		}
		return
	}

	if kv.Config.Shards[shard.Id]!=kv.gid{
		kv.PrintLog("Give me wrong shard,shardId=%d,should belong to group %d",shard.Id,kv.Config.Shards[shard.Id])
	}

	kv.Shards[shard.Id]=shard

	for ckid,opid:=range args.LatestCommitId{
		if myopid,ok:=kv.LatestCommitId[ckid];ok{
			if opid>myopid{
				kv.LatestCommitId[ckid]=opid
			}
		}else{
			kv.LatestCommitId[ckid]=opid
		}
	}

	if replyCh!=nil{
		reply.WrongLeader=false
		replyCh<-reply
		kv.PrintLog("(master) success receive shard %d",shard.Id)
	}else{
		kv.PrintLog("success receive shard %d",shard.Id)
	}
	kv.ConfigComplete=kv.IsConfigComplete()
}

func (kv *ShardKV) SendShardMigrationRPC(servers []string,args *ShardMigrationArgs) bool{
	for _, server := range servers {
		s := kv.make_end(server)
		var reply ShardMigrationReply

		ok := s.Call("ShardKV.ShardMigrationRPC", args, &reply)
		if ok && !reply.WrongLeader {
			return true
		}
	}
	return false
}

func (kv *ShardKV) GarbageCollector(){
	var replyCh chan int
	for{
		if _,isLeader:=kv.rf.GetState();!isLeader{
			//kv.PrintLog("garbage collector release kv lock")
			time.Sleep(GarbageCollectorInterval*1000*time.Millisecond)
			continue
		}

		replyCh=make(chan int,100)
		timer:=time.NewTimer(GarbageCollectorInterval*1000*time.Millisecond)
		kv.mu.Lock()
		//kv.PrintLog("garbage collector get kv lock")
		if kv.isDead{
			kv.mu.Unlock()
			//kv.PrintLog("garbage collector release kv lock")
			return
		}
		//kv.PrintLog("garbage collector call raft getstate")

		for _,garbage:=range kv.Garbages{
			var args ShardMigrationArgs

			args.LatestCommitId = garbage.LatestCommitId
			args.ConfigNum = garbage.ConfigNum
			args.MyShard = garbage.Shard
			go func(replyCh chan int, args *ShardMigrationArgs, servers []string) {
				if kv.SendShardMigrationRPC(servers, args) {
					kv.PrintLog("move shard %d success", args.MyShard.Id)
					replyCh <- args.MyShard.Id
				}
			}(replyCh, &args, garbage.Servers)

		}
		kv.mu.Unlock()
		//kv.PrintLog("garbage collector release kv lock")

		Loop:
		for{
			select {
			case <-timer.C:
				break Loop
			case shardId := <-replyCh:
				var op Op

				op.MyType = DeleteGarbageShardOp
				op.Args = DeleteGarbageShardArgs{shardId}
				//kv.PrintLog("garbage collector call raft start")
				if _, _, isLeader := kv.rf.Start(op); !isLeader {
					break Loop
				}
			}
		}
	}
}

func (kv *ShardKV) RunDeleteGarbageShard(op Op){
	kv.mu.Lock()
	//defer kv.PrintLog("quit in run delete")
	defer kv.mu.Unlock()

	//kv.PrintLog("in run delete garbage shard")
	args:=op.Args.(DeleteGarbageShardArgs)
	delete(kv.Garbages,args.ShardId)
	kv.PrintLog("delete shard %d from garbage",args.ShardId)
}

func (kv *ShardKV) RunReconfigure(op Op){
	kv.mu.Lock()
	//defer kv.PrintLog("quit run reconfigure")
	defer kv.mu.Unlock()

	//kv.PrintLog("in run reconfigure")
	newConfig:=op.Args.(ReConfigureArgs).Config
	if newConfig.Num<=kv.Config.Num{
		return
	}
	kv.PrintLog("start reconfig, old %v, new %v",kv.Config.Shards,newConfig.Shards)
	oldConfig:=kv.Config
	kv.Config=newConfig
	kv.ConfigComplete=false

	latestCommitId:=make(map[int64]int)
	for ckid,opid:=range kv.LatestCommitId{
		latestCommitId[ckid]=opid
	}

	for shardId,shard:=range kv.Shards{
		if kv.Config.Shards[shardId]!=kv.gid{
			var garbage Garbage

			garbage.ConfigNum=newConfig.Num
			garbage.Servers=kv.Config.Groups[kv.Config.Shards[shardId]]
			//garbage.Gid=kv.Config.Shards[shardId]
			garbage.Shard=shard
			garbage.LatestCommitId=latestCommitId
			kv.Garbages[shardId]=garbage
			delete(kv.Shards,shardId)
		}
	}

	kv.PrintLog("after reconfig:my shards %v",kv.Shards)

	for shardId,gid:=range oldConfig.Shards{
		if gid==0 && kv.Config.Shards[shardId]==kv.gid{
			var shard Shard
			shard.Make(shardId)
			kv.Shards[shardId]=shard
		}
	}

	kv.ConfigComplete=kv.IsConfigComplete()
}

func (kv *ShardKV) RunGet(op Op){
	kv.mu.Lock()
	//defer kv.PrintLog("quit run get")
	defer kv.mu.Unlock()

	//kv.PrintLog("in run get")
	var reply GetReply
	var replyCh chan GetReply
	var args GetArgs

	if op.LeaderId==kv.me && op.replyCh!=nil{
		replyCh=op.replyCh.(chan GetReply)
		args=op.Args.(GetArgs)
	}else{
		return
	}

	if args.ConfigNum!=kv.Config.Num{
		kv.PrintLog("my latest config %d not responisble for key=%s",kv.Config.Num,args.Key)
		reply.Err=ErrWrongGroup
		replyCh<-reply
		return
	}

	if shard,ok:=kv.Shards[args.ShardId];ok{
		var isExist bool
		reply.Value,isExist=shard.Get(args.Key)
		if isExist{
			reply.Err=OK
		}else{
			reply.Err=ErrNoKey
		}
		reply.WrongLeader=false
		kv.PrintLog("success Get: key=%s,value=%s",args.Key,reply.Value)
	}else{
		kv.PrintLog("not compelete config %d",kv.Config.Num)
		reply.Err=ErrWrongGroup
	}
	replyCh<-reply
}

func (kv *ShardKV) RunPutAppend(op Op){
	kv.mu.Lock()
	//defer kv.PrintLog("quit tun putappend")
	defer kv.mu.Unlock()

	//kv.PrintLog("in run put append")
	var reply PutAppendReply
	var replyCh chan PutAppendReply
	args:=op.Args.(PutAppendArgs)

	if op.LeaderId==kv.me && op.replyCh!=nil{
		replyCh=op.replyCh.(chan PutAppendReply)
	}

	if args.ConfigNum!=kv.Config.Num{
		if replyCh!=nil{
			reply.Err=ErrWrongGroup
			replyCh<-reply
			kv.PrintLog("give reply to wrong group PutAppend")
		}
		return
	}

	if shard,ok:=kv.Shards[args.ShardId];ok{
		if opId, ok := kv.LatestCommitId[args.CkId]; ok && opId >= args.OpId {
			if replyCh != nil {
				reply.WrongLeader = false
				reply.Err = OK
				replyCh <- reply
				kv.PrintLog("give reply to duplicate PutAppend, commitId=%d, opid=%d", opId, args.OpId)
			}
			return
		} else {
			kv.LatestCommitId[args.CkId] = args.OpId
		}

		if op.MyType==PutOp{
			shard.Put(args.Key,args.Value)
		}else{
			shard.Append(args.Key,args.Value)
		}
		reply.WrongLeader=false
		reply.Err=OK

	}else{
		reply.Err=ErrWrongGroup
	}

	if replyCh!=nil{
		if reply.Err==OK{
			kv.PrintLog("success PutAppend: key=%s,value=%s",args.Key,args.Value)
		}else{
			kv.PrintLog("not compelete config %d",kv.Config.Num)
		}
		replyCh<-reply
	}
}

func (kv *ShardKV) ApplyRoutine(){
	for{
		//kv.PrintLog("in apply routine")
		kv.mu.Lock()
		if kv.isDead{
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()

		applyMsg:=<-kv.applyCh
		kv.PrintLog("receive apply index %d",applyMsg.Index)
		if applyMsg.UseSnapshot{
			kv.mu.Lock()
			kv.Shards=make(map[int]Shard)
			kv.Garbages=make(map[int]Garbage)

			reader := bytes.NewBuffer(kv.persister.ReadSnapshot())
			decoder := gob.NewDecoder(reader)
			err:=decoder.Decode(kv)
			if err!=nil{
				log.Fatal("encode:", err)
			}

			kv.PrintLog("After use snapshot...")
			kv.PrintLog("LatestCommitId:%v", kv.LatestCommitId)
			kv.PrintLog("Shards:%v", kv.Shards)
			kv.PrintLog("Config:%v", kv.Config)
			kv.PrintLog("Garbages:%v", kv.Garbages)
			kv.PrintLog("ConfigComplete:%v", kv.ConfigComplete)
			kv.mu.Unlock()
		}else{
			//kv.PrintLog("receive raft apply")
			op:=applyMsg.Command.(Op)
			switch(op.MyType){
			case ReConfigureOp:
				kv.RunReconfigure(op)
			case DeleteGarbageShardOp:
				kv.RunDeleteGarbageShard(op)
			case MigrationOp:
				kv.RunShardMigration(op)
			case GetOp:
				kv.RunGet(op)
			case PutOp:
				fallthrough
			case AppendOp:
				kv.RunPutAppend(op)
			}
			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				kv.mu.Lock()
				//kv.PrintLog("begin snapshot,size=%d",kv.persister.RaftStateSize())
				writer := new(bytes.Buffer)
				encoder := gob.NewEncoder(writer)
				err:=encoder.Encode(kv)
				if err != nil {
					log.Fatal("encode:", err)
				}
				snapshot := writer.Bytes()
				if kv.rf.GarbageCollect(applyMsg.Index){
					kv.PrintLog("make snapshot...")
					kv.PrintLog("LatestCommitId:%v", kv.LatestCommitId)
					kv.PrintLog("Shards:%v", kv.Shards)
					kv.PrintLog("Config:%v", kv.Config)
					kv.PrintLog("Garbages:%v", kv.Garbages)
					kv.PrintLog("ConfigComplete:%v", kv.ConfigComplete)
					kv.persister.SaveSnapshot(snapshot)
				}
				//kv.PrintLog("finish snapshot,size=%d",kv.persister.RaftStateSize())
				kv.mu.Unlock()
			}
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
	gob.Register(GetArgs{})
	gob.Register(PutAppendArgs{})
	gob.Register(DeleteGarbageShardArgs{})
	gob.Register(ShardMigrationArgs{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg,100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.isDead=false
	kv.LatestCommitId=make(map[int64]int)
	kv.mck=shardmaster.MakeClerk(kv.masters)
	kv.Shards=make(map[int]Shard)
	kv.persister=persister
	kv.ConfigComplete=true
	kv.Garbages=make(map[int]Garbage)

	reader:=bytes.NewBuffer(kv.persister.ReadSnapshot())
	decoder:=gob.NewDecoder(reader)
	decoder.Decode(kv)

	kv.PrintLog("init...")
	kv.PrintLog("LatestCommitId:%v",kv.LatestCommitId)
	kv.PrintLog("Shards:%v",kv.Shards)
	kv.PrintLog("Config:%v",kv.Config)
	kv.PrintLog("Garbages:%v",kv.Garbages)
	kv.PrintLog("ConfigComplete:%v",kv.ConfigComplete)

	go kv.ConfigMonitor()
	go kv.GarbageCollector()
	go kv.ApplyRoutine()

	return kv
}
