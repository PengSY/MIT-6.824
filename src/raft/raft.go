package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"time"
	"fmt"
	"math/rand"
)

// import "bytes"
// import "encoding/gob"

const secondtonano=1000000000
const heartbeatInterval=0.15

const LEADER=0
const CANDIDATE=1
const FOLLOWER=2
type Role int64

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
//LogEntry struct:hold information for each log entry
//
type LogEntry struct{
	Command interface{}
	Term int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
//AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct{
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

//
//AppendEntries RPC reply structure.
//
type AppendEntriesReply struct{
	Term int
	Success bool
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persistent state
	currentTerm int
	votedFor int
	log []LogEntry

	//Volatile state on all servers
	commitIndex int
	lastApplied int

	//Volatile state on leaders
	nextIndex []int
	matchIndex []int

	//whether I believes I am the leader
	leaderId int

	//election timer
	electionTimer *time.Timer

	applyCh chan ApplyMsg

	role Role

	isDead bool

	/*
	replyIndexCh chan int
	replyArray []AppendEntriesReply
	newMatchIdx int
	successNum int
	*/
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term=rf.currentTerm
	isleader=(rf.role==LEADER)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()

	reply.Term=rf.currentTerm
	//reset votedFor whenever currentTerm is reset
	if(args.Term>rf.currentTerm){
		rf.currentTerm=args.Term
		rf.votedFor=-1
		if rf.role==LEADER{
			//rf.PrintLog("find larger term in RequestVote, step down to follower")
			rf.leaderId=-1
			rf.role=FOLLOWER
			go rf.ElectionRoutine()
		}else if rf.role==CANDIDATE{
			rf.role=FOLLOWER
		}
	}

	var lastLogIndex,lastLogTerm int

	lastLogIndex=len(rf.log)
	if lastLogIndex>0{
		lastLogTerm=rf.log[lastLogIndex-1].Term
	}

	if  rf.votedFor>=0 || args.Term < rf.currentTerm || args.LastLogTerm<lastLogTerm || (args.LastLogTerm==lastLogTerm && args.LastLogIndex<lastLogIndex){
		rf.PrintLog(fmt.Sprintf("I did not vote for s%d",args.CandidateId))
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	rf.PrintLog(fmt.Sprintf("I vote for s%d",args.CandidateId))
	ResetTimer(rf.electionTimer)
	reply.VoteGranted=true
	rf.votedFor=args.CandidateId
	//rf.PrintLog(fmt.Sprintf("current term=%d, votedFor=%d",rf.currentTerm,rf.votedFor))

	rf.mu.Unlock()
}

//
//AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	reply.Term=rf.currentTerm
	if args.Term<rf.currentTerm{
		reply.Success=false
		rf.mu.Unlock()
		return
	}
	ResetTimer(rf.electionTimer)
	//rf.PrintLog(fmt.Sprintf("receive AppendEnriesRPC from s%d",args.LeaderId))
	if rf.currentTerm<args.Term{
		rf.currentTerm=args.Term
		rf.votedFor=-1
		if rf.role==CANDIDATE{
			rf.role=FOLLOWER
		}else if rf.role==LEADER{
			//rf.PrintLog(fmt.Sprintf("find larger term in append rpc from s%d, step down to follower",args.LeaderId))
			rf.role=FOLLOWER
			go rf.ElectionRoutine()
		}
	}
	if rf.leaderId!=args.LeaderId{
		rf.leaderId=args.LeaderId
	}

	prevLogIndex:=args.PrevLogIndex

	if prevLogIndex>len(rf.log){
		rf.PrintLog(fmt.Sprintf("append rpc from s%d fail, because of inconsistence, prevLogIndex=%d, len(rf.log)=%d",
			args.LeaderId,prevLogIndex,len(rf.log)))
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	if (prevLogIndex>0 && rf.log[prevLogIndex-1].Term!=args.PrevLogTerm) {
		rf.PrintLog(fmt.Sprintf("append rpc from s%d fail, because of inconsistence, prevLogIndex=%d, rf.log[prevLogIndex].Term=%d, args.prevLogTerm=%d",
			args.LeaderId,prevLogIndex,rf.log[prevLogIndex-1].Term,args.PrevLogTerm))
		rf.log=rf.log[:prevLogIndex-1]
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	reply.Success=true
	entries:=args.Entries
	for i,entry:=range entries{
		index:=prevLogIndex+1+i
		if index>len(rf.log){
			rf.log=append(rf.log,entries[i:]...)
			break
		}
		if rf.log[index-1].Term!=entry.Term{
			rf.log=rf.log[:index-1]
			rf.log=append(rf.log,entries[i:]...)
			break
		}

	}

	var newCommitIndex int

	if args.LeaderCommit > len(rf.log) {
		newCommitIndex = len(rf.log)
	} else {
		newCommitIndex = args.LeaderCommit
	}

	if rf.commitIndex<newCommitIndex{
		rf.commitIndex=newCommitIndex
		rf.PrintLog(fmt.Sprintf("update commit to %d",rf.commitIndex))
		//go rf.ApplyRoutine()

		var entriesApply []LogEntry
		var startIndex int
		if rf.lastApplied < rf.commitIndex {
			//entriesApply = make([]LogEntry, rf.commitIndex - rf.lastApplied)
			//copy(entriesApply, rf.log[rf.lastApplied:rf.commitIndex])
			entriesApply = rf.log[rf.lastApplied:rf.commitIndex]
			startIndex = rf.lastApplied + 1
			rf.lastApplied = rf.commitIndex
		}
		rf.Apply(startIndex, entriesApply)
	}
	if len(entries)==0{
		//rf.PrintLog(fmt.Sprintf("recieve heartbeat from s%d",args.LeaderId))
	}else{
		rf.PrintLog(fmt.Sprintf("append rpc from s%d success, append entries [%d,%d]",
			args.LeaderId, prevLogIndex + 1, len(rf.log)))
	}

	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
//code to send a AppendEntries RPC to a server.
//
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok :=rf.peers[server].Call("Raft.AppendEntries",args,reply)
	return ok
}

//
//log function for debug
//
func (rf *Raft) PrintLog(s string){
	if rf.isDead{
		return
	}
	fmt.Println(fmt.Sprintf("s%d (Term=%d,LeaderId=%d):",rf.me,rf.currentTerm,rf.leaderId)+s)
}

func ResetTimer(timer *time.Timer){
	//if timer had expired, clear channel
	select{
	case <-timer.C:
	default:
	}
	timer.Reset(time.Duration((rand.Float64()+0.9)*secondtonano))
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	isLeader=(rf.role==LEADER)
	if isLeader{
		term=rf.currentTerm
		e:=LogEntry{command,term}
		rf.log=append(rf.log,e)
		index=len(rf.log)
		rf.PrintLog(fmt.Sprintf("(leader) append log entry(term=%d,index=%d,command=%d)",term,index,command))
		//rf.BroadcastAppendEntriesRPC()
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.isDead=true
}

func (rf *Raft) BroadcastRequestVoteRPC()(chan int,[]RequestVoteReply){
	rf.mu.Lock()

	replyIndexCh:=make(chan int,len(rf.peers)-1)
	replyArray:=make([]RequestVoteReply,len(rf.peers))

	rf.role=CANDIDATE
	rf.currentTerm++
	rf.votedFor=rf.me
	rf.leaderId=-1

	rf.PrintLog("election timeout")

	//rf.electionTimer.Reset(time.Duration((rand.Float64() + 2) * secondtonano))
	ResetTimer(rf.electionTimer)
	var args RequestVoteArgs
	args.Term=rf.currentTerm
	args.CandidateId=rf.me
	args.LastLogIndex=len(rf.log)
	if args.LastLogIndex>0{
		args.LastLogTerm=rf.log[args.LastLogIndex-1].Term
	}
	me:=rf.me
	peerNum:=len(rf.peers)
	rf.mu.Unlock()

	for i:=0;i<peerNum;i=i+1{
		if i==me{
			continue
		}
		go func(index int){
			rf.PrintLog(fmt.Sprintf("send RequestVote RPC to s%d",index))
			res:=rf.sendRequestVote(index,&args,&replyArray[index])
			if res{
				replyIndexCh<-index
			}else{
				replyIndexCh<--1
			}

		}(i)
	}

	return replyIndexCh,replyArray
}

//
//broadcast heartbeat
//
/*
func(rf *Raft) BroadcastHeartbeat(routineTerm int)(chan int,[]AppendEntriesReply,bool){
	rf.mu.Lock()
	var replyIndexCh chan int
	var replyArray []AppendEntriesReply

	if rf.currentTerm!=routineTerm || rf.isDead{
		rf.mu.Unlock()
		return replyIndexCh,replyArray,true
	}

	replyIndexCh=make(chan int,len(rf.peers)-1)
	replyArray=make([]AppendEntriesReply,len(rf.peers))
	leaderId:=rf.leaderId
	log:=rf.log
	currentTerm:=rf.currentTerm
	commitIndex:=rf.commitIndex
	nextIndexArray:=make([]int,len(rf.nextIndex))
	copy(nextIndexArray,rf.nextIndex)
	peerNum:=len(rf.peers)

	rf.mu.Unlock()

	for i:=0;i<peerNum;i++{
		if i==leaderId{
			continue
		}

		nextIndex:=nextIndexArray[i]

		var args AppendEntriesArgs
		args.Term=currentTerm
		args.LeaderId=leaderId
		args.LeaderCommit=commitIndex
		args.PrevLogIndex=nextIndex-1
		if args.PrevLogIndex>0{
			args.PrevLogTerm=log[args.PrevLogIndex-1].Term
		}

		go func(index int,args AppendEntriesArgs) {
			//rf.PrintLog(fmt.Sprintf("(leader) send heartbeat to s%d (prevLogIndex=%d)",
			//	index,args.PrevLogIndex))
			res := rf.sendAppendEntries(index, &args, &replyArray[index])
			if res {
				replyIndexCh <- index
			} else {
				replyIndexCh <- -1
			}
		}(i,args)
	}

	return replyIndexCh,replyArray,false
}
*/

func (rf *Raft) BroadcastAppendEntriesRPC(routineTerm int)(chan int,[]AppendEntriesReply,int,bool){
	var replyIndexCh chan int
	var replyArray []AppendEntriesReply
	var newMatchIdx int

	rf.mu.Lock()

	if rf.currentTerm != routineTerm || rf.isDead {
		rf.mu.Unlock()
		return replyIndexCh,replyArray,newMatchIdx,true
	}

	replyIndexCh=make(chan int,len(rf.peers)-1)
	replyArray=make([]AppendEntriesReply,len(rf.peers))
	newMatchIdx=len(rf.log)
	leaderId:=rf.leaderId
	log:=rf.log
	currentTerm:=rf.currentTerm
	commitIndex:=rf.commitIndex
	peerNum:=len(rf.peers)

	rf.mu.Unlock()

	for i:=0;i<peerNum;i++{
		if i==leaderId{
			continue
		}

		nextIndex:=rf.nextIndex[i]

		/*
		if nextIndex==rf.newMatchIdx+1{
			replyIndexCh<--1
			rf.successNum++
			continue
		}
		*/

		var args AppendEntriesArgs
		args.Term=currentTerm
		args.LeaderId=leaderId
		args.LeaderCommit=commitIndex
		args.PrevLogIndex=nextIndex-1
		if args.PrevLogIndex>0{
			args.PrevLogTerm=log[args.PrevLogIndex-1].Term
		}
		if len(log)>0 && nextIndex<=newMatchIdx{
			args.Entries=log[nextIndex-1:newMatchIdx]
		}

		go func(index int,args AppendEntriesArgs) {
			if len(args.Entries)==0{
				rf.PrintLog(fmt.Sprintf("(leader) send heartbeat to s%d",index))
			}else{
				rf.PrintLog(fmt.Sprintf("(leader) send append rpc to s%d (index %d to index %d,prevLogIndex=%d)",
					index, nextIndex, len(log), args.PrevLogIndex))
			}

			res := rf.sendAppendEntries(index, &args, &replyArray[index])
			if res {
				replyIndexCh <- index
			} else {
				replyIndexCh <- -1
			}
		}(i,args)
	}

	return replyIndexCh,replyArray,newMatchIdx,false
}

func (rf *Raft) HandleVoteReply(reply RequestVoteReply,voteCount *int)(bool){
	rf.mu.Lock()

	if rf.role!=CANDIDATE{
		rf.mu.Unlock()
		return false
	}

	term:=reply.Term
	voteGranted:=reply.VoteGranted

	if voteGranted{
		(*voteCount)++
		rf.PrintLog("receive vote")
		if *voteCount > len(rf.peers) / 2 {

			rf.PrintLog("I become the leader")
			rf.leaderId = rf.me
			rf.role = LEADER
			for i := 0; i < len(rf.peers); i++ {
				if (i == rf.me) {
					continue
				}
				rf.nextIndex[i] = len(rf.log) + 1
				rf.matchIndex[i] = 0
			}

			//rf.SwitchToLeader()
			routineTerm:=rf.currentTerm
			rf.mu.Unlock()
			go rf.ReplicateLogRoutine(routineTerm)
			return true
		}
	}else if term>rf.currentTerm{
		rf.currentTerm=term
		rf.votedFor=-1
		rf.role=FOLLOWER
	}

	rf.mu.Unlock()
	return false
}

/*
func (rf *Raft) HandleHeartbeatReply(reply AppendEntriesReply,routineTerm int){
	rf.mu.Lock()
	if rf.currentTerm!=routineTerm || rf.isDead{
		rf.mu.Unlock()
		return
	}

	if !reply.Success && reply.Term > rf.currentTerm {
		rf.role = FOLLOWER
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.mu.Unlock()
		go rf.ElectionRoutine()
		return
	}
	rf.mu.Unlock()
}
*/

func (rf *Raft) HandleAppendEntriesReply(peerIdx int,reply AppendEntriesReply,newMatchIdx int,successNum *int,routineTerm int)(bool){
	rf.mu.Lock()

	if rf.currentTerm != routineTerm || rf.isDead {
		rf.mu.Unlock()
		return true
	}

	if !reply.Success && reply.Term > rf.currentTerm {
		rf.role = FOLLOWER
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		go rf.ElectionRoutine()
		rf.mu.Unlock()
		return false
	} else if !reply.Success {
		rf.nextIndex[peerIdx]--
		rf.PrintLog(fmt.Sprintf("(leader) receive append reply from s%d, decrease nextIndex to %d",
			peerIdx, rf.nextIndex[peerIdx]))
	} else {
		/*if rf.newMatchIdx==0{
			return
		}else */
		if newMatchIdx>rf.matchIndex[peerIdx]{
			rf.nextIndex[peerIdx]=newMatchIdx+1
			rf.matchIndex[peerIdx]=newMatchIdx
			rf.PrintLog(fmt.Sprintf("(leader) receive append reply from s%d, nextIndex=%d, matchIndex=%d",
				peerIdx, rf.nextIndex[peerIdx], rf.matchIndex[peerIdx]))
		}
		(*successNum)++
		if *successNum>len(rf.peers)/2{
			if rf.commitIndex<newMatchIdx{
				rf.PrintLog(fmt.Sprintf("(leader) update commitIndex form %d to %d,successNum=%d",
					rf.commitIndex,newMatchIdx,*successNum))
				rf.commitIndex=newMatchIdx
			}else{
				rf.mu.Unlock()
				return false
			}

			var entriesApply []LogEntry
			var startIndex int
			if rf.lastApplied < rf.commitIndex {
				//entriesApply = make([]LogEntry, rf.commitIndex - rf.lastApplied)
				//copy(entriesApply, rf.log[rf.lastApplied:rf.commitIndex])
				entriesApply = rf.log[rf.lastApplied:rf.commitIndex]
				startIndex = rf.lastApplied + 1
				rf.lastApplied = rf.commitIndex
			}
			rf.Apply(startIndex,entriesApply)
		}
	}

	rf.mu.Unlock()
	return false
}

/*
func (rf *Raft) SwitchToLeader(){

	rf.PrintLog("I become the leader")
	rf.leaderId = rf.me
	rf.role=LEADER
	for i:=0;i<len(rf.peers);i++{
		if(i==rf.me){
			continue
		}
		rf.nextIndex[i]=len(rf.log)+1
		rf.matchIndex[i]=0
	}
	routineTerm:=rf.currentTerm
	//rf.mu.Unlock()
	//go rf.HeartbeatRoutine()
	go rf.ReplicateLogRoutine(routineTerm)
	//go rf.UpdateCommitIndexRoutine(routineTerm)
	//go rf.HeartbeatRoutine(routineTerm)
	//go rf.LeaderRoutine(routineTerm)
}
*/

/*
func (rf *Raft) HeartbeatRoutine(routineTerm int){
	ticker:=time.NewTicker(time.Duration((heartbeatInterval*secondtonano)))
	var replyIndexCh chan int
	var replyArray []AppendEntriesReply
	var isReturn bool

	for{
		select{
		case <-ticker.C:
			replyIndexCh, replyArray, isReturn= rf.BroadcastHeartbeat(routineTerm)
			if isReturn{
				return
			}
		case index:=<-replyIndexCh:
			if index>0{
				rf.HandleHeartbeatReply(replyArray[index],routineTerm)
			}
		}
	}

}
*/

func (rf *Raft) ReplicateLogRoutine(routineTerm int){
	ticker:=time.NewTicker(time.Duration(heartbeatInterval*secondtonano))
	var replyIndexCh chan int
	var replyArray []AppendEntriesReply
	var newMatchIdx int
	var successNum int
	var isReturn bool

	for{
		time.Sleep(10*time.Millisecond)
		select{
		case <-ticker.C:
			successNum = 1
			replyIndexCh,replyArray,newMatchIdx,isReturn=rf.BroadcastAppendEntriesRPC(routineTerm)
		case index:=<-replyIndexCh:
			if index>=0{
				isReturn=rf.HandleAppendEntriesReply(index,replyArray[index],newMatchIdx,&successNum,routineTerm)
			}
		}
		if isReturn{
			return
		}

	}
}


//
//responsible for sending heartbeat and replicate logs
//
/*
func (rf *Raft) LeaderRoutine(routineTerm int){
	ticker:=time.NewTicker(time.Duration((heartbeatInterval*secondtonano)))
	var replyIndexCh chan int
	var replyArray []AppendEntriesReply
	var isReturn bool
	var newMatchIdx int
	var successNum int

	for{
		select {
		case <-ticker.C:
			replyIndexCh, replyArray, isReturn = rf.BroadcastHeartbeat(routineTerm)
			newMatchIdx=0
		default:
			successNum=1
			replyIndexCh, replyArray, newMatchIdx,isReturn= rf.BroadcastAppendEntriesRPC(routineTerm,&successNum)
		}
		if isReturn {
			return
		}
		for i:=0;i<len(replyArray)-1;i++{
			index:=<-replyIndexCh
			if index>=0{
				if(rf.HandleAppendEntriesReply(replyArray[index], index, newMatchIdx, routineTerm,&successNum)){
					return
				}
			}
		}
	}
}
*/

/*
func (rf *Raft) UpdateCommitIndexRoutine(routineTerm int){
	for{
		rf.mu.Lock()
		if rf.role!=LEADER || rf.currentTerm!=routineTerm || rf.isDead{
			rf.mu.Unlock()
			return
		}
		matchIndexArray:=make([]int,len(rf.matchIndex))
		copy(matchIndexArray,rf.matchIndex)
		peerNum:=len(rf.peers)
		me:=rf.me
		commitIndex:=rf.commitIndex
		indexNum:=len(rf.log)-commitIndex
		if indexNum<=0{
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()

		count:=make([]int,indexNum)

		newCommitIndex:=commitIndex
		for i:=0;i<peerNum;i++{
			if i==me{
				continue
			}
			matchIndex:=matchIndexArray[i]
			if matchIndex<=commitIndex{
				continue
			}
			count[matchIndex-(commitIndex+1)]++
			if count[matchIndex-(commitIndex+1)]+1>peerNum/2 && newCommitIndex<matchIndex{
				newCommitIndex=matchIndex
			}
		}

		if newCommitIndex>commitIndex{
			rf.mu.Lock()
			if rf.commitIndex<newCommitIndex{
				rf.commitIndex=newCommitIndex
				rf.PrintLog(fmt.Sprintf("(leader) update commitIndex form %d to %d",rf.commitIndex,newCommitIndex))
			}
			//go rf.ApplyRoutine()

			var entriesApply []LogEntry
			var startIndex int
			if rf.lastApplied < rf.commitIndex {
				//entriesApply = make([]LogEntry, rf.commitIndex - rf.lastApplied)
				//copy(entriesApply, rf.log[rf.lastApplied:rf.commitIndex])
				entriesApply = rf.log[rf.lastApplied:rf.commitIndex]
				startIndex = rf.lastApplied + 1
				rf.lastApplied = rf.commitIndex
			}
			go rf.Apply(startIndex,entriesApply)

			rf.mu.Unlock()
		}
	}
}
*/

//
//Every time election timeout, ElectionRoutine broadcast
//RequestVote rpc and waiting for reply. Send command STOP/
//Reset through commandCh
//
func (rf *Raft) ElectionRoutine(){
	//init election timer within 2 to 3 seconds
	//timer:=time.NewTimer(time.Duration((rand.Float64()+2)*secondtonano))
	var replyIndexCh chan int
	var replyArray []RequestVoteReply
	var voteCount int

	rf.mu.Lock()
	rf.electionTimer=time.NewTimer(time.Duration((rand.Float64()+0.9)*secondtonano))
	rf.mu.Unlock()

	for{
		time.Sleep(10*time.Millisecond)
		select{
		case <-rf.electionTimer.C:
			replyIndexCh,replyArray=rf.BroadcastRequestVoteRPC()
			voteCount=1
		case index:=<-replyIndexCh:
			if index>0{
				if(rf.HandleVoteReply(replyArray[index],&voteCount)){
					return
				}
			}
		/*
			if voteCount>len(replyArray)/2{
				rf.SwitchToLeader()
				return
			}
			*/
		}
		if rf.isDead{
			return
		}
	}

}


func (rf *Raft) Apply(startIndex int,entriesApply []LogEntry){
	for i, entry := range entriesApply {
		var msg ApplyMsg
		msg.Command = entry.Command
		msg.Index = startIndex + i
		rf.applyCh <- msg
		rf.PrintLog(fmt.Sprintf("apply index %d", msg.Index))
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.currentTerm=0
	rf.commitIndex=0
	rf.lastApplied=0
	rf.nextIndex=make([]int,len(peers))
	rf.matchIndex=make([]int,len(peers))
	rf.leaderId=-1
	rf.votedFor=-1
	rf.role=FOLLOWER
	rf.applyCh=applyCh

	rf.isDead=false

	rand.Seed(time.Now().UnixNano())
	go rf.ElectionRoutine()

	return rf
}
