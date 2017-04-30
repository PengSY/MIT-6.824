package raft

type RaftLog struct{
	Log []LogEntry
}

func (log *RaftLog) Make(){
	log.Log=make([]LogEntry,1)
	log.Log[0].Index=0
	log.Log[0].Term=0
}

func (log *RaftLog) GetLogEntry(index int) LogEntry{
	return log.Log[index-log.Log[0].Index]
}

func (log *RaftLog) GetLogEntries(begin int,end int) []LogEntry{
	prevIndex:=log.Log[0].Index
	return log.Log[begin-prevIndex:end-prevIndex]
}

func (log *RaftLog) GetLogEntriesBefore(index int) []LogEntry{
	return log.Log[:index-log.Log[0].Index]
}

func (log *RaftLog) GetLogEntriesAfter(index int) []LogEntry{
	return log.Log[index-log.Log[0].Index:]
}

func (log *RaftLog) AppendLogEntry(logEntry LogEntry){
	log.Log=append(log.Log,logEntry)
}

func (log *RaftLog) AppendLogEntries(logEntries []LogEntry){
	log.Log=append(log.Log,logEntries...)
}

func (log *RaftLog) SetHeadLogEntry(index int,term int){
	log.Log[0].Index=index
	log.Log[0].Term=term
}

func (log *RaftLog) SetLog(logEntries []LogEntry){
	log.Log=logEntries
}

func (log *RaftLog) GetLength() int{
	return len(log.Log)
}

func (log *RaftLog) GetLastLogEntry() LogEntry{
	return log.Log[log.GetLength()-1]
}

func (log *RaftLog) GetLastLogEntryIndex() int{
	return log.Log[log.GetLength()-1].Index
}

func (log *RaftLog) GetLastLogEntryTerm() int{
	return log.Log[log.GetLength()-1].Term
}

func (log *RaftLog) GetHeadLogEntryIndex() int{
	return log.Log[0].Index
}

func (log *RaftLog) GetHeadLogEntryTerm() int{
	return log.Log[0].Term
}