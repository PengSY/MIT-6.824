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

import (
	"labrpc"
	"time"
	"fmt"
	"math/rand"
	"bytes"
	"encoding/gob"
	"sync"
)

const RaftDebug=0
const heartbeatInterval=50
const UseSnapshot=0

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
	Index int
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
	FirstConflictIndex int
}

type InstallSnapshotArgs struct{
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Data []byte
}

type InstallSnapshotReply struct{
	Term int
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
	CurrentTerm int
	VotedFor int
	Log RaftLog

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
	commitCh chan int
	snapshotCh chan int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term=rf.CurrentTerm
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

	//rf.PrintLog(fmt.Sprintf("persist: CurrentTerm=%d,VotedFor=%d,LogLen=%d,LastLog=%d",
		//rf.CurrentTerm,rf.VotedFor,len(rf.Log),lastLog))

	w:=new(bytes.Buffer)
	e:=gob.NewEncoder(w)
	e.Encode(rf)
	data:=w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r:=bytes.NewBuffer(data)
	d:=gob.NewDecoder(r)
	d.Decode(rf)

	//rf.PrintLog(fmt.Sprintf("readPersist: CurrentTerm=%d,VotedFor=%d,LogLen=%d,LastLog=%d",
		//rf.CurrentTerm,rf.VotedFor,len(rf.Log),lastLog))
}

func (rf *Raft) InstallSnapshotRPC(args *InstallSnapshotArgs,reply *InstallSnapshotReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term=rf.CurrentTerm
	if args.Term<rf.CurrentTerm{
		return
	}
	if rf.CurrentTerm<args.Term{
		rf.CurrentTerm=args.Term
		rf.VotedFor=-1
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

	rf.PrintLog(fmt.Sprintf("recive snapshot from s%d",args.LeaderId))

	ResetTimer(rf.electionTimer)
	if args.LeaderId!=rf.leaderId{
		rf.leaderId=args.LeaderId
	}
	if args.LastIncludedIndex<=rf.Log.GetLastLogEntryIndex() && rf.Log.GetLogEntry(args.LastIncludedIndex).Term==args.LastIncludedTerm{
		rf.PrintLog("already have last included index of snapshot")
		return
	}
	rf.persister.SaveSnapshot(args.Data)
	rf.lastApplied=args.LastIncludedIndex
	rf.commitIndex=args.LastIncludedIndex
	rf.Log.SetLog(make([]LogEntry,1))
	rf.Log.SetHeadLogEntry(args.LastIncludedIndex,args.LastIncludedTerm)

	//var snapshotMsg SnapshotMsg
	//snapshotMsg.snapshot=args.Data
	rf.snapshotCh<-UseSnapshot

	rf.PrintLog("snapshot done")

	rf.persist()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()

	reply.Term=rf.CurrentTerm
	//reset votedFor whenever currentTerm is reset
	if(args.Term>rf.CurrentTerm){
		rf.CurrentTerm=args.Term
		rf.VotedFor=-1
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

	lastLogIndex=rf.Log.GetLastLogEntryIndex()
	lastLogTerm=rf.Log.GetLastLogEntryTerm()

	if  rf.VotedFor>=0 || args.Term < rf.CurrentTerm || args.LastLogTerm<lastLogTerm || (args.LastLogTerm==lastLogTerm && args.LastLogIndex<lastLogIndex){
		rf.PrintLog(fmt.Sprintf("I did not vote for s%d",args.CandidateId))
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	rf.PrintLog(fmt.Sprintf("I vote for s%d",args.CandidateId))
	ResetTimer(rf.electionTimer)
	reply.VoteGranted=true
	rf.VotedFor=args.CandidateId
	//rf.PrintLog(fmt.Sprintf("current term=%d, votedFor=%d",rf.CurrentTerm,rf.VotedFor))

	rf.persist()

	rf.mu.Unlock()
}

//
//AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()

	needPersist:=false

	reply.Term=rf.CurrentTerm
	if args.Term<rf.CurrentTerm{
		reply.Success=false
		rf.mu.Unlock()
		return
	}
	ResetTimer(rf.electionTimer)
	//rf.PrintLog(fmt.Sprintf("receive AppendEnriesRPC from s%d",args.LeaderId))
	if rf.CurrentTerm<args.Term{
		rf.CurrentTerm=args.Term
		rf.VotedFor=-1
		needPersist=true
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

	if prevLogIndex>rf.Log.GetLastLogEntryIndex(){
		//rf.PrintLog(fmt.Sprintf("append rpc from s%d fail, because of inconsistence, prevLogIndex=%d, len(rf.Log)=%d",
			//args.LeaderId,prevLogIndex,len(rf.Log)))
		reply.Success = false

		reply.FirstConflictIndex=rf.Log.GetLastLogEntryIndex()+1
		//reply.ConflictTerm=rf.Log.GetLastLogEntryTerm()

		if needPersist{
			rf.persist()
		}

		rf.mu.Unlock()
		return
	}
	rf.PrintLog(fmt.Sprintf("headLogIndex=%d,prevLogIndex=%d",rf.Log.GetHeadLogEntryIndex(),prevLogIndex))

	if (prevLogIndex>=rf.Log.GetHeadLogEntryIndex() && rf.Log.GetLogEntry(prevLogIndex).Term!=args.PrevLogTerm) {
		//rf.PrintLog(fmt.Sprintf("append rpc from s%d fail, because of inconsistence, prevLogIndex=%d, rf.Log[prevLogIndex].Term=%d, args.prevLogTerm=%d",
			//args.LeaderId,prevLogIndex,rf.Log[prevLogIndex-1].Term,args.PrevLogTerm))
		reply.Success = false
		conflictTerm:=rf.Log.GetLogEntry(prevLogIndex).Term
		//reply.ConflictTerm=rf.Log.GetLogEntry(prevLogIndex).Term
		var idx int
		for idx=prevLogIndex;idx>=rf.Log.GetHeadLogEntryIndex();idx--{
			if rf.Log.GetLogEntry(idx).Term!=conflictTerm{
				break
			}
		}
		reply.FirstConflictIndex=idx+1

		if needPersist{
			rf.persist()
		}

		rf.mu.Unlock()
		return
	}

	reply.Success=true
	entries:=args.Entries
	for i,entry:=range entries{
		needPersist=true
		index:=prevLogIndex+1+i
		if index<=rf.Log.GetHeadLogEntryIndex(){
			continue
		}
		if index>rf.Log.GetLastLogEntryIndex(){
			rf.Log.AppendLogEntries(entries[i:])
			rf.PrintLog(fmt.Sprintf("entries[%d:]:%v",i,entries[i:]))
			rf.PrintLog(fmt.Sprintf("append success,log:%v",rf.Log.Log))
			break
		}
		if rf.Log.GetLogEntry(index).Term!=entry.Term{
			rf.Log.SetLog(rf.Log.GetLogEntriesBefore(index))
			rf.Log.AppendLogEntries(entries[i:])
			break
		}
	}


	if len(entries)==0{
		//rf.PrintLog(fmt.Sprintf("recieve heartbeat from s%d",args.LeaderId))
	}else{
		rf.PrintLog(fmt.Sprintf("append rpc from s%d success, append entries [%d,%d]",
			args.LeaderId, prevLogIndex + 1, rf.Log.GetLastLogEntryIndex()))
	}



	if needPersist{
		rf.persist()
	}

	var newCommitIndex int

	if args.LeaderCommit > rf.Log.GetLastLogEntryIndex() {
		newCommitIndex = rf.Log.GetLastLogEntryIndex()
	} else {
		newCommitIndex = args.LeaderCommit
	}

	if rf.commitIndex<newCommitIndex{
		rf.commitIndex=newCommitIndex
		rf.PrintLog(fmt.Sprintf("update commit to %d",rf.commitIndex))
		rf.commitCh<-rf.commitIndex

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

func (rf *Raft) sendInstallSnapshotRPC(server int,args *InstallSnapshotArgs,reply *InstallSnapshotReply) bool{
	ok:=rf.peers[server].Call("Raft.InstallSnapshotRPC",args,reply)
	return ok
}

//
//log function for debug
//
func (rf *Raft) PrintLog(s string){
	if RaftDebug==0 || rf.isDead{
		return
	}
	fmt.Println(fmt.Sprintf("s%d (Term=%d,LeaderId=%d):",rf.me,rf.CurrentTerm,rf.leaderId)+s)
}

func ResetTimer(timer *time.Timer){
	//if timer had expired, clear channel
	select{
	case <-timer.C:
	default:
	}
	timer.Reset(time.Duration(time.Duration(rand.Intn(150)+150)*time.Millisecond))
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
		term=rf.CurrentTerm
		index=rf.Log.GetLastLogEntryIndex()+1
		e:=LogEntry{command,term,index}
		rf.Log.AppendLogEntry(e)
		rf.PrintLog(fmt.Sprintf("(leader) append log entry(term=%d,index=%d)",term,index))
		rf.persist()
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
	//rf.PrintLog("raft:I am dead")
	rf.isDead=true
}

func (rf *Raft) BroadcastRequestVoteRPC()(chan int,[]RequestVoteReply,bool){
	rf.mu.Lock()

	replyIndexCh:=make(chan int,len(rf.peers)-1)
	replyArray:=make([]RequestVoteReply,len(rf.peers))
	if rf.isDead {
		rf.mu.Unlock()
		return replyIndexCh,replyArray,true
	}

	rf.role=CANDIDATE
	rf.CurrentTerm++
	rf.VotedFor=rf.me
	rf.leaderId=-1

	//rf.PrintLog("election timeout")

	ResetTimer(rf.electionTimer)
	var args RequestVoteArgs
	args.Term=rf.CurrentTerm
	args.CandidateId=rf.me
	args.LastLogIndex=rf.Log.GetLastLogEntryIndex()
	args.LastLogTerm=rf.Log.GetLastLogEntryTerm()
	me:=rf.me
	peerNum:=len(rf.peers)

	rf.persist()

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

	return replyIndexCh,replyArray,false
}

func (rf *Raft) BroadcastAppendEntriesRPC(routineTerm int)(chan int,[]AppendEntriesReply,int,bool){
	var replyIndexCh chan int
	var replyArray []AppendEntriesReply
	var newMatchIdx int

	rf.mu.Lock()

	if rf.CurrentTerm != routineTerm || rf.isDead{
		rf.mu.Unlock()
		return replyIndexCh,replyArray,newMatchIdx,true
	}

	replyIndexCh=make(chan int,len(rf.peers)-1)
	replyArray=make([]AppendEntriesReply,len(rf.peers))
	newMatchIdx=rf.Log.GetLastLogEntryIndex()
	leaderId:=rf.leaderId
	currentTerm:=rf.CurrentTerm
	commitIndex:=rf.commitIndex
	peerNum:=len(rf.peers)

	for i:=0;i<peerNum;i++{
		if i==leaderId{
			continue
		}

		nextIndex:=rf.nextIndex[i]

		if nextIndex<=rf.Log.GetHeadLogEntryIndex(){
			var args InstallSnapshotArgs
			args.Data = rf.persister.ReadSnapshot()
			args.LastIncludedIndex = rf.Log.GetHeadLogEntryIndex()
			args.LastIncludedTerm = rf.Log.GetHeadLogEntryTerm()
			args.LeaderId = rf.me
			args.Term = rf.CurrentTerm
			rf.PrintLog(fmt.Sprintf("nextIndex=%d,send snapshot to s%d",nextIndex,i))
			go func(index int, args InstallSnapshotArgs) {
				var reply InstallSnapshotReply
				res := rf.sendInstallSnapshotRPC(index, &args, &reply)
				if res {
					rf.HandleInstallSnapshotReply(reply,index)
				}
			}(i, args)
			continue
		}

		var args AppendEntriesArgs
		args.Term = currentTerm
		args.LeaderId = leaderId
		args.LeaderCommit = commitIndex
		args.PrevLogIndex = nextIndex - 1
		args.PrevLogTerm = rf.Log.GetLogEntry(args.PrevLogIndex).Term

		if nextIndex <= newMatchIdx {
			args.Entries=rf.Log.GetLogEntries(nextIndex,newMatchIdx+1)
		}

		go func(index int,args AppendEntriesArgs) {

			if len(args.Entries)==0{
				//rf.PrintLog(fmt.Sprintf("(leader) send heartbeat to s%d",index))
			}else{
				rf.PrintLog(fmt.Sprintf("(leader) send append rpc to s%d from index %d",index, nextIndex))
			}


			res := rf.sendAppendEntries(index, &args, &replyArray[index])
			if res {
				replyIndexCh <- index
			} else {
				replyIndexCh <- -1
				rf.PrintLog(fmt.Sprintf("not receive reply from s%d",index))
			}
		}(i,args)
	}

	rf.mu.Unlock()
	return replyIndexCh,replyArray,newMatchIdx,false
}

func (rf *Raft) HandleVoteReply(reply RequestVoteReply,voteCount *int)(bool){
	rf.mu.Lock()

	if rf.role!=CANDIDATE{
		rf.mu.Unlock()
		return false
	}
	if rf.isDead{
		rf.mu.Unlock()
		return true
	}

	term:=reply.Term
	voteGranted:=reply.VoteGranted

	if voteGranted{
		(*voteCount)++
		//rf.PrintLog("receive vote")
		if *voteCount > len(rf.peers) / 2 {
			rf.PrintLog("I become the leader")
			rf.leaderId = rf.me
			rf.role = LEADER
			for i := 0; i < len(rf.peers); i++ {
				if (i == rf.me) {
					continue
				}
				rf.nextIndex[i] = rf.Log.GetLastLogEntryIndex()+1
				rf.matchIndex[i] = 0
			}

			routineTerm:=rf.CurrentTerm
			rf.mu.Unlock()
			go rf.ReplicateLogRoutine(routineTerm)
			return true
		}
	}else if term>rf.CurrentTerm{
		rf.CurrentTerm=term
		rf.VotedFor=-1
		rf.role=FOLLOWER
		rf.persist()
	}

	rf.mu.Unlock()
	return false
}

func (rf *Raft) HandleAppendEntriesReply(peerIdx int,reply AppendEntriesReply,newMatchIdx int,successNum *int,routineTerm int)(bool){
	rf.mu.Lock()

	if rf.CurrentTerm != routineTerm || rf.isDead{
		rf.mu.Unlock()
		return true
	}

	if !reply.Success && reply.Term > rf.CurrentTerm {
		rf.role = FOLLOWER
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		go rf.ElectionRoutine()
		rf.persist()
		rf.mu.Unlock()
		return false
	} else if !reply.Success {
		rf.nextIndex[peerIdx]=reply.FirstConflictIndex
		rf.PrintLog(fmt.Sprintf("(leader) receive append reply from s%d, decrease nextIndex to %d",
			peerIdx, rf.nextIndex[peerIdx]))
	} else {
		if newMatchIdx>rf.matchIndex[peerIdx]{
			rf.nextIndex[peerIdx]=newMatchIdx+1
			rf.matchIndex[peerIdx]=newMatchIdx
			rf.PrintLog(fmt.Sprintf("(leader) receive append reply from s%d, nextIndex=%d, matchIndex=%d",
				peerIdx, rf.nextIndex[peerIdx], rf.matchIndex[peerIdx]))
		}
		(*successNum)++
		if *successNum>len(rf.peers)/2{
			//as figure8 says
			if rf.commitIndex<newMatchIdx && rf.Log.GetLogEntry(newMatchIdx).Term==rf.CurrentTerm{
				rf.PrintLog(fmt.Sprintf("(leader) update commitIndex form %d to %d,successNum=%d",
					rf.commitIndex,newMatchIdx,*successNum))
				rf.commitIndex=newMatchIdx
			}else{
				rf.mu.Unlock()
				return false
			}

			if rf.lastApplied<rf.commitIndex{
				rf.commitCh<-rf.commitIndex
			}
		}
	}

	rf.mu.Unlock()
	return false
}

func (rf *Raft) HandleInstallSnapshotReply(reply InstallSnapshotReply,id int){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.CurrentTerm {
		rf.role = FOLLOWER
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		go rf.ElectionRoutine()
		rf.persist()
	}
	if rf.nextIndex[id]<=rf.Log.GetHeadLogEntryIndex(){
		rf.nextIndex[id]=rf.Log.GetHeadLogEntryIndex()+1
	}
}

func (rf *Raft) ReplicateLogRoutine(routineTerm int){
	ticker:=time.NewTicker(time.Duration(heartbeatInterval*time.Millisecond))
	var replyIndexCh chan int
	var replyArray []AppendEntriesReply
	var newMatchIdx int
	var successNum int
	var isReturn bool

	for{

		select{
		case <-ticker.C:
			successNum = 1
			replyIndexCh,replyArray,newMatchIdx,isReturn=rf.BroadcastAppendEntriesRPC(routineTerm)
			if isReturn{
				return
			}
		case index:=<-replyIndexCh:
			if index>=0{
				if(rf.HandleAppendEntriesReply(index,replyArray[index],newMatchIdx,&successNum,routineTerm)){
					return
				}
			}
		}
	}
}

//
//Every time election timeout, ElectionRoutine broadcast
//RequestVote rpc and waiting for reply. Send command STOP/
//Reset through commandCh
//
func (rf *Raft) ElectionRoutine(){
	var replyIndexCh chan int
	var replyArray []RequestVoteReply
	var voteCount int
	var isReturn bool

	rf.mu.Lock()
	rf.electionTimer=time.NewTimer(time.Duration(rand.Intn(150)+150)*time.Millisecond)
	rf.mu.Unlock()

	for{
		select{
		case <-rf.electionTimer.C:
			replyIndexCh,replyArray,isReturn=rf.BroadcastRequestVoteRPC()
			if isReturn{
				return
			}
			voteCount=1
		case index:=<-replyIndexCh:
			if index>=0{
				if(rf.HandleVoteReply(replyArray[index],&voteCount)){
					return
				}
			}
		}
	}

}

func (rf *Raft) ApplyRoutine(){
	for{
		select {
		case commitIndex:=<-rf.commitCh:
			rf.mu.Lock()
			if rf.isDead{
				rf.mu.Unlock()
				return
			}
			if rf.lastApplied>=commitIndex{
				rf.mu.Unlock()
				continue
			}
			rf.PrintLog(fmt.Sprintf("headIndex=%d,lastApplied=%d,commitIndex=%d",rf.Log.GetHeadLogEntryIndex(),rf.lastApplied,commitIndex))
			entries:=rf.Log.GetLogEntries(rf.lastApplied+1,commitIndex+1)
			rf.lastApplied = commitIndex
			for _, entry := range entries {
				var msg ApplyMsg
				msg.Command = entry.Command
				msg.Index = entry.Index
				msg.UseSnapshot=false
				rf.applyCh <- msg
				rf.PrintLog(fmt.Sprintf("apply index %d", msg.Index))
			}
			rf.mu.Unlock()
		case <-rf.snapshotCh:
			var msg ApplyMsg
			msg.UseSnapshot=true
			rf.mu.Lock()
			if rf.isDead{
				rf.mu.Unlock()
				return
			}
			rf.applyCh<-msg
			rf.mu.Unlock()
			//rf.PrintLog("send snap shot msg to kvserver")
		}
	}
}

func (rf *Raft) GarbageCollect(endIndex int){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.Log.GetHeadLogEntryIndex()>=endIndex{
		return
	}
	rf.Log.SetLog(rf.Log.GetLogEntriesAfter(endIndex))
	rf.persist()
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
	rf.CurrentTerm=0
	//rf.commitIndex=0
	//rf.lastApplied=0
	rf.nextIndex=make([]int,len(peers))
	rf.matchIndex=make([]int,len(peers))
	rf.leaderId=-1
	rf.VotedFor=-1
	rf.role=FOLLOWER
	rf.applyCh=applyCh
	rf.isDead=false
	rf.commitCh=make(chan int,100)
	rf.snapshotCh=make(chan int,10)
	rf.Log.Make()

	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex=rf.Log.GetHeadLogEntryIndex()
	rf.lastApplied=rf.commitIndex

	rand.Seed(time.Now().UnixNano())
	go rf.ElectionRoutine()
	go rf.ApplyRoutine()

	return rf
}
