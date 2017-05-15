package shardmaster


import "raft"
import "labrpc"
import "sync"
import (
	"encoding/gob"
	"time"
)


const (
	JoinOp="Join"
	LeaveOp="Leave"
	MoveOp="Move"
	QueryOp="Query"
)

type OpType string

const ResendTimeout=0.1

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

	configs []Config // indexed by config num
}

type Pair struct {
	Key int
	Value int
}

type Op struct {
	// Your data here.
	MyType OpType
	Args interface{}
	replyCh interface{}
	LeaderId int
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	var op Op
	var replyCh chan JoinReply

	replyCh=make(chan JoinReply)
	op.Args=*args
	op.MyType=JoinOp
	op.replyCh=replyCh
	op.LeaderId=sm.me
	timer:=time.NewTimer(ResendTimeout*1000*time.Millisecond)
	if _,_,isLeader:=sm.rf.Start(op);!isLeader{
		reply.WrongLeader=true
		return
	}
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

	replyCh=make(chan LeaveReply)
	op.MyType=LeaveOp
	op.Args=*args
	op.replyCh=replyCh
	op.LeaderId=sm.me
	timer:=time.NewTimer(ResendTimeout*1000*time.Millisecond)
	if _,_,isLeader:=sm.rf.Start(op);!isLeader{
		reply.WrongLeader=true
		return
	}
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

	replyCh=make(chan MoveReply)
	op.MyType=MoveOp
	op.Args=*args
	op.replyCh=replyCh
	op.LeaderId=sm.me
	timer:=time.NewTimer(ResendTimeout*1000*time.Millisecond)
	if _,_,isLeader:=sm.rf.Start(op);!isLeader{
		reply.WrongLeader=true
		return
	}
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

	replyCh=make(chan QueryReply)
	op.MyType=QueryOp
	op.Args=*args
	op.replyCh=replyCh
	op.LeaderId=sm.me
	timer:=time.NewTimer(ResendTimeout*1000*time.Millisecond)
	if _,_,isLeader:=sm.rf.Start(op);!isLeader{
		reply.WrongLeader=true
		return
	}
	select {
	case <-timer.C:
		reply.WrongLeader=true
	case *reply=<-replyCh:
	}
}

func (sm *ShardMaster) CountShardsPerGroup()()

func (sm *ShardMaster) RunJoin(op Op){
	var reply JoinReply
	var replyCh chan JoinReply
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


}

func (sm *ShardMaster) RunLeave(op Op){

}

func (sm *ShardMaster) RunMove(op Op){

}

func (sm *ShardMaster) RunQuery(op Op){

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
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.

	return sm
}
