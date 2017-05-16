package shardmaster


import "raft"
import "labrpc"
import "sync"
import (
	"encoding/gob"
	"time"
	"sort"
	"fmt"
)

const ShardMasterDebug=0

const (
	JoinOp="Join"
	LeaveOp="Leave"
	MoveOp="Move"
	QueryOp="Query"
)

type OpType string

const ResendTimeout=0.5

const Kill=0
type KillMsg int

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	lastOpId map[int64]int
	killCh chan KillMsg
	isDead bool

	configs []Config // indexed by config num
}

type Pair struct {
	Key int
	Value int
}

type PairList []Pair

func (pl PairList) Len() int{
	return len(pl)
}

func (pl PairList) Less(i,j int) bool{
	return pl[i].Value<pl[j].Value
}

func (pl PairList) Swap(i,j int){
	pl[i],pl[j]=pl[j],pl[i]
}

type Op struct {
	// Your data here.
	MyType OpType
	Args interface{}
	replyCh interface{}
	LeaderId int
}

func (sm *ShardMaster) PrintLog(format string, a ...interface{}){
	if ShardMasterDebug==0 || sm.isDead{
		return
	}
	fmt.Println(fmt.Sprintf("s%d:",sm.me)+fmt.Sprintf(format,a...))
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	var op Op
	var replyCh chan JoinReply

	replyCh=make(chan JoinReply,1)
	op.Args=*args
	op.MyType=JoinOp
	op.replyCh=replyCh
	op.LeaderId=sm.me
	timer:=time.NewTimer(ResendTimeout*1000*time.Millisecond)
	if _,_,isLeader:=sm.rf.Start(op);!isLeader{
		reply.WrongLeader=true
		return
	}
	sm.PrintLog("receive Join Request from c%d, waiting aggreement",args.CkId)
	select {
	case <-timer.C:
		reply.WrongLeader=true
	case *reply=<-replyCh:
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	var op Op
	var replyCh chan LeaveReply

	replyCh=make(chan LeaveReply,1)
	op.MyType=LeaveOp
	op.Args=*args
	op.replyCh=replyCh
	op.LeaderId=sm.me
	timer:=time.NewTimer(ResendTimeout*1000*time.Millisecond)
	if _,_,isLeader:=sm.rf.Start(op);!isLeader{
		reply.WrongLeader=true
		return
	}
	sm.PrintLog("receive Leave Request from c%d, waiting aggreement",args.CkId)
	select {
	case <-timer.C:
		reply.WrongLeader=true
	case *reply=<-replyCh:
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	var op Op
	var replyCh chan MoveReply

	replyCh=make(chan MoveReply,1)
	op.MyType=MoveOp
	op.Args=*args
	op.replyCh=replyCh
	op.LeaderId=sm.me
	timer:=time.NewTimer(ResendTimeout*1000*time.Millisecond)
	if _,_,isLeader:=sm.rf.Start(op);!isLeader{
		reply.WrongLeader=true
		return
	}
	sm.PrintLog("receive Move Request from c%d, waiting aggreement",args.CkId)
	select {
	case <-timer.C:
		reply.WrongLeader=true
	case *reply=<-replyCh:
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	var op Op
	var replyCh chan QueryReply

	replyCh=make(chan QueryReply,1)
	op.MyType=QueryOp
	op.Args=*args
	op.replyCh=replyCh
	op.LeaderId=sm.me
	timer:=time.NewTimer(ResendTimeout*1000*time.Millisecond)
	if _,_,isLeader:=sm.rf.Start(op);!isLeader{
		reply.WrongLeader=true
		return
	}
	sm.PrintLog("receive Query Request from c%d, waiting aggreement",args.CkId)
	select {
	case <-timer.C:
		reply.WrongLeader=true
		sm.PrintLog("query request timeout")
	case *reply=<-replyCh:
	}
}

func (sm *ShardMaster) CountShardsPerGroup() PairList{
	var pl PairList
	countMap:=make(map[int]int)

	lastConfig:=sm.configs[len(sm.configs)-1]
	pl=make(PairList,len(sm.configs[len(sm.configs)-1].Groups))

	groups:=lastConfig.Groups
	for gid:=range groups{
		countMap[gid]=0
	}

	shards:=lastConfig.Shards
	for _,gid:=range shards{
		if gid!=0{
			countMap[gid]++
		}
	}

	i:=0
	for gid,count:=range countMap{
		pl[i]=Pair{gid,count}
		i++
	}
	sort.Sort(sort.Reverse(pl))
	return pl
}

func (sm *ShardMaster) Rebalance(decreaseMap map[int]int,increaseMap map[int]int) [NShards]int{
	newShards:=sm.configs[len(sm.configs)-1].Shards

	sm.PrintLog("decreaseMap:%v",decreaseMap)
	sm.PrintLog("increaseMap:%v",increaseMap)
	for shardId,gid:=range newShards{
		if diff,ok:=decreaseMap[gid];ok{
			newShards[shardId]=0
			diff--
			if diff<=0{
				delete(decreaseMap,gid)
			}else{
				decreaseMap[gid]=diff
			}
		}
	}

	shardId:=0
	for gid,diff:=range increaseMap{
		for ;diff>0;diff--{
			for ;newShards[shardId]!=0;shardId++{}
			newShards[shardId]=gid
		}
	}
	return newShards
}

func (sm *ShardMaster) RunJoin(op Op){
	var reply JoinReply
	var replyCh chan JoinReply
	var newConfig Config
	args:=op.Args.(JoinArgs)

	if op.LeaderId==sm.me && op.replyCh!=nil{
		replyCh=op.replyCh.(chan JoinReply)
	}
	if opId,ok:=sm.lastOpId[args.CkId];ok && opId==args.OpId{
		if replyCh!=nil{
			reply.WrongLeader=false
			reply.Err=OK
			replyCh<-reply
		}
		return
	}else{
		sm.lastOpId[args.CkId]=args.OpId
	}

	newConfig.Groups=make(map[int][]string)
	for gid,servers:=range sm.configs[len(sm.configs)-1].Groups{
		newConfig.Groups[gid]=servers
	}
	for gid,servers:=range args.Servers{
		if _,ok:=sm.configs[len(sm.configs)-1].Groups[gid];ok{
			delete(args.Servers,gid)
		}
		newConfig.Groups[gid]=servers
	}

	countPair:=sm.CountShardsPerGroup()

	sm.PrintLog("countPair:%d",countPair)
	newGroupNum:=len(countPair)+len(args.Servers)
	base:=NShards/newGroupNum
	mod:=NShards%newGroupNum
	sm.PrintLog("newGroupNum=%d,base=%d,mod=%d",newGroupNum,base,mod)

	decreaseMap:=make(map[int]int)
	increaseMap:=make(map[int]int)

	for _,pair:=range countPair{
		if mod>0{
			if pair.Value>base+1{
				decreaseMap[pair.Key]=pair.Value-(base+1)
			}
			mod--
		}else if pair.Value>base{
			decreaseMap[pair.Key]=pair.Value-base
		}
	}
	for gid:=range args.Servers{
		if mod>0{
			increaseMap[gid]=base+1
			mod--
		}else if base>0{
			increaseMap[gid]=base
		}
	}

	newConfig.Shards=sm.Rebalance(decreaseMap,increaseMap)
	sm.PrintLog("new shards:%v",newConfig.Shards)
	newConfig.Num=len(sm.configs)

	sm.configs=append(sm.configs,newConfig)

	if replyCh != nil {
		reply.WrongLeader = false
		reply.Err = OK
		replyCh <- reply
	}
}

func (sm *ShardMaster) RunLeave(op Op){
	var reply LeaveReply
	var replyCh chan LeaveReply
	var newConfig Config
	args:=op.Args.(LeaveArgs)

	if op.LeaderId==sm.me && op.replyCh!=nil{
		replyCh=op.replyCh.(chan LeaveReply)
	}
	if opId,ok:=sm.lastOpId[args.CkId];ok && opId==args.OpId{
		if replyCh!=nil{
			reply.WrongLeader=false
			reply.Err=OK
			replyCh<-reply
		}
		return
	}else{
		sm.lastOpId[args.CkId]=args.OpId
	}

	newConfig.Groups=make(map[int][]string)
	for gid,servers:=range sm.configs[len(sm.configs)-1].Groups{
		newConfig.Groups[gid]=servers
	}
	for idx,gid:=range args.GIDs{
		if _,ok:=sm.configs[len(sm.configs)-1].Groups[gid];!ok{
			args.GIDs=append(args.GIDs[:idx],args.GIDs[idx+1:]...)
		}else{
			delete(newConfig.Groups,gid)
		}
	}

	countPair:=sm.CountShardsPerGroup()

	newGroupNum:=len(countPair)-len(args.GIDs)
	base:=NShards/newGroupNum
	mod:=NShards%newGroupNum

	decreaseMap:=make(map[int]int)
	increaseMap:=make(map[int]int)

	for _,gid:=range args.GIDs{
		decreaseMap[gid]=0
	}
	for _,pair:=range countPair{
		if _,ok:=decreaseMap[pair.Key];ok{
			if pair.Value>0{
				decreaseMap[pair.Key]=pair.Value
			}else{
				delete(decreaseMap,pair.Key)
			}
		}else{
			if mod>0{
				if pair.Value<base+1{
					increaseMap[pair.Key]=base+1-pair.Value
				}
				mod--
			}else if pair.Value<base{
				increaseMap[pair.Key]=base-pair.Value
			}
		}
	}

	newConfig.Shards=sm.Rebalance(decreaseMap,increaseMap)
	sm.PrintLog("new shards:%v",newConfig.Shards)
	newConfig.Num=len(sm.configs)

	sm.configs=append(sm.configs,newConfig)

	if replyCh != nil {
		reply.WrongLeader = false
		reply.Err = OK
		replyCh <- reply
	}
}

func (sm *ShardMaster) RunMove(op Op){
	var reply MoveReply
	var replyCh chan MoveReply
	var newConfig Config
	args:=op.Args.(MoveArgs)

	if op.LeaderId==sm.me && op.replyCh!=nil{
		replyCh=op.replyCh.(chan MoveReply)
	}
	if opId,ok:=sm.lastOpId[args.CkId];ok && opId==args.OpId{
		if replyCh!=nil{
			reply.WrongLeader=false
			reply.Err=OK
			replyCh<-reply
		}
		return
	}else{
		sm.lastOpId[args.CkId]=args.OpId
	}

	newConfig.Num=len(sm.configs)
	newConfig.Shards=sm.configs[len(sm.configs)-1].Shards
	newConfig.Shards[args.Shard]=args.GID
	sm.PrintLog("new shards:%v",newConfig.Shards)
	newConfig.Groups=sm.configs[len(sm.configs)-1].Groups
	sm.configs=append(sm.configs,newConfig)

	if replyCh != nil {
		reply.WrongLeader = false
		reply.Err = OK
		replyCh <- reply
	}
}

func (sm *ShardMaster) RunQuery(op Op){
	var reply QueryReply
	var replyCh chan QueryReply
	args:=op.Args.(QueryArgs)

	sm.PrintLog("query request reached agreement")
	if op.LeaderId==sm.me && op.replyCh!=nil{
		replyCh=op.replyCh.(chan QueryReply)
	}else{
		return
	}

	if args.Num==-1 || args.Num>=len(sm.configs){
		reply.Config=sm.configs[len(sm.configs)-1]
	}else{
		reply.Config=sm.configs[args.Num]
	}
	reply.WrongLeader=false
	reply.Err=OK
	replyCh<-reply
	sm.PrintLog("return query request")
}

func (sm *ShardMaster) ApplyRoutine(){
	for{
		select {
		case <-sm.killCh:
			return
		case applyMsg:=<-sm.applyCh:
			op:=applyMsg.Command.(Op)
			switch op.MyType{
			case JoinOp:
				sm.RunJoin(op)
			case LeaveOp:
				sm.RunLeave(op)
			case MoveOp:
				sm.RunMove(op)
			case QueryOp:
				sm.RunQuery(op)
			}
		}
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	sm.killCh<-Kill
	sm.isDead=true
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(QueryArgs{})
	gob.Register(MoveArgs{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.killCh=make(chan KillMsg)
	sm.lastOpId=make(map[int64]int)
	sm.isDead=false
	go sm.ApplyRoutine()

	return sm
}
