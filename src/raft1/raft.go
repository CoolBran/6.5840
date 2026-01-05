package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

/*
	note all

0.[leader electrion:]after send vote request:a.check myself is candidate now. a(true):1.vote, voteCnt++, check become leader now 2.not vote, reply.Term > rf.currentTerm, become follower
1. a server vote to other: update the heartbeat time, delay the election time(the same partition have candidate)

2. Term divide with log Term
3. [important] heartbeat: appendEntries with empty log entry[without log add, use old log entries]
4. appendEntry divide two type: heartbeat[empty log entry to all] and log add[with log to special server about matchIndex && nextIndex]
5. matchIndex && nextIndex[init], update[update]
6. after follower vote, update the voteFor, think of when it reset -1(for next vote) ==> filed voteFor, voteCnt could reset while use.(Lazy think)
7. think of the Log Compression
8.
*/
type LogEntry struct {
	Term    int
	Index   int //first index is 1
	Command interface{}
}

type raftRole int

const (
	Follower raftRole = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent state on all servers
	currentTerm int //initialize to 0 on first boot, increases monotonically
	votedFor    int //candidateId that received vote in current term (or null if none)
	logEntries  []LogEntry

	//volatile state on all servers
	heartbeatTime time.Time
	peersCnt      int
	voteCnt       int
	role          raftRole

	commitIndex int //index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// //volatile state on leaders
	nextIndex  []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	applyCh        chan raftapi.ApplyMsg
	applyCond      *sync.Cond   // condition variable for apply goroutine
	replicatorCond []*sync.Cond // condition variable for replicator goroutine

	// retCh chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	return w.Bytes()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	rf.persister.Save(rf.encodeState(), rf.persister.ReadSnapshot())
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	rf.persister.Save(rf.encodeState(), snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		DPrintf("{Node %v} fails to decode persisted state", rf.me)
	}
	rf.currentTerm, rf.votedFor, rf.logEntries = currentTerm, votedFor, logs
	rf.lastApplied, rf.commitIndex = rf.getFirstLog().Index, rf.getFirstLog().Index
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex := rf.getFirstLog().Index
	if index <= snapshotIndex || index > rf.getLastLog().Index {
		DPrintf("{Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", rf.me, index, snapshotIndex, rf.currentTerm)
		return
	}
	// remove log entries up to index
	rf.logEntries = shrinkEntries(rf.logEntries[index-snapshotIndex:])
	rf.logEntries[0].Command = nil
	rf.lastApplied, rf.commitIndex = index, index
	rf.persister.Save(rf.encodeState(), snapshot)
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot with index %v", rf.me, rf.role, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), index)

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	//term smaller than me
	if args.Term < rf.currentTerm {
		DPrintf("[follower:%v][voteFailed, candidateTerm smaller than me]:deny to vote to %v\n", rf.me, args.CandidateId)
		return
	}

	//todotodotodotodotodo:see later important(think more)
	if args.Term > rf.currentTerm { //voteFor reset when new Term
		rf.becomeFollower()
		rf.currentTerm = args.Term
		rf.persist()
	}

	//already vote [args.Term >= rf.currentTerm]
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DPrintf("[follower:%v][voteFailed, already vote]:deny to vote to %v\n", rf.me, args.CandidateId)
		return
	}

	//log older than me
	if !rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		DPrintf("[follower:%v][voteFailed, log older than me]:deny to vote to %v\n", rf.me, args.CandidateId)
		return
	}

	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId
	rf.persist()
	rf.heartbeatTime = time.Now() //note: vote to other, update heartbeat time, delay the leader election time

	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	DPrintf("[follower:%v][voteSuccess]:vote to %v\n", rf.me, args.CandidateId)

}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//§5.1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	//normal status
	rf.heartbeatTime = time.Now() //note: heartbeat, update heartbeat time
	rf.currentTerm = args.Term    //Term divide with log Term
	rf.persist()
	if rf.role != Follower {
		rf.becomeFollower()
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	//§5.3 reply false if log doesn't contain an entry at prevLogIndex
	if args.PrevLogIndex < rf.getFirstLog().Index {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if !rf.isLogMatched(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	//put the log entry into the rf.logEntries
	if len(args.Entries) > 0 {
		DPrintf("[follower:%v][get appendEntries]:lastLog:%v, logEntries:%v\n", rf.me, rf.getLastLog(), args.Entries)
	}
	rf.logEntries = append(rf.logEntries[:args.PrevLogIndex-rf.getFirstLog().Index+1], args.Entries...)
	rf.persist()
	DPrintf("[follower:%v][after appendEntries]:%v\n", rf.me, rf.logEntries)
	//commit log index
	newCommitIndex := min(args.LeaderCommit, rf.getLastLog().Index)
	if newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}
	reply.Term, reply.Success = rf.currentTerm, true
}

func (rf *Raft) becomeFollower() {
	rf.votedFor = -1 //todo: reset vote, time think(while term change)
	rf.persist()
	rf.voteCnt = 0
	rf.role = Follower
}

func (rf *Raft) becomeCandidate() {
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.voteCnt = 1 //voteCnt assign 1, doesn't use ++
}

func (rf *Raft) becomeLeader() {
	rf.role = Leader
	rf.voteCnt = 0
	rf.persist()
	DPrintf("[Candidate==>Leader:%v]:Term: %v \n", rf.me, rf.currentTerm)
}

// Error Error
// func (rf *Raft) checkLogOlderMe(lastLogTerm int, lastLogIndex int) bool {
// 	LogSize := len(rf.logEntries)
// 	if lastLogTerm < rf.logEntries[LogSize-1].Term {
// 		return true
// 	}
// 	if lastLogIndex < rf.logEntries[LogSize-1].Index {
// 		return true
// 	}
// 	return false
// }

func (rf *Raft) isLogUpToDate(index, term int) bool {
	lastLog := rf.getLastLog()
	return term > lastLog.Term || (term == lastLog.Term && index >= lastLog.Index)
}

// sendRequestVoteToOneWithLock --> sendRequestVote(rpc caller) --> RequestVote (rpc callee)
func (rf *Raft) sendRequestVoteToOneWithLock(index int) {

	rf.mu.Lock()
	args := rf.genRequestVoteArgs()
	rf.mu.Unlock()

	reply := RequestVoteReply{}
	if ok := rf.sendRequestVote(index, args, &reply); ok { //net level
		if reply.VoteGranted {
			rf.mu.Lock()
			rf.voteCnt++
			if rf.voteCnt*2 > rf.peersCnt && rf.role == Candidate {
				rf.becomeLeader()
				for i := 0; i < rf.peersCnt; i++ {
					if i != rf.me {
						go rf.sendEmptyAppendEntriesToOneWithLock(i)
					}
				}
			}
			rf.mu.Unlock()
		}
		return
	}

}

// sendEmptyAppendEntriesToOneWithLock(empty, this level decided empty or not) --> sendAppendEntries(rpc caller) --> AppendEntries (rpc callee)
func (rf *Raft) sendEmptyAppendEntriesToOneWithLock(peer int) { //leader use
	rf.mu.Lock()
	args := rf.genAppendEntriesArgs(rf.getLastLog().Index)
	reply := &AppendEntriesReply{}
	DPrintf("[Leader:%v][empty appendEntries]:send to %v, match: %v next: %v, logSize: %v,logEntry:%v,\n", rf.me, peer, rf.matchIndex[peer], rf.nextIndex[peer], len(args.Entries), args.Entries)
	rf.mu.Unlock()

	if ok := rf.sendAppendEntries(peer, args, reply); ok { //net level
		if !reply.Success { //todo: reply.Term > rf.currentTerm, consider other situation
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower()
			}
			rf.mu.Unlock()
		}

	}
}

// todo: important
func (rf *Raft) sendAppendEntriesToOneWithLock(peer int) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.matchIndex[peer]
	if prevLogIndex < rf.getFirstLog().Index {
		// only send InstallSnapshot RPC
		args := rf.genInstallSnapshotArgs()
		rf.mu.Unlock()
		reply := new(InstallSnapshotReply)
		if rf.sendInstallSnapshot(peer, args, reply) {
			rf.mu.Lock()
			if rf.role == Leader && rf.currentTerm == args.Term {
				if reply.Term > rf.currentTerm {
					rf.becomeFollower()
					rf.currentTerm, rf.votedFor = reply.Term, -1
					rf.persist()
				} else {
					rf.nextIndex[peer] = args.LastIncludedIndex + 1
					rf.matchIndex[peer] = args.LastIncludedIndex
				}
			}
			rf.mu.Unlock()
			DPrintf("{Node %v} sends InstallSnapshotArgs %v to {Node %v} and receives InstallSnapshotReply %v", rf.me, args, peer, reply)
		}
	} else {
		args := rf.genAppendEntriesArgs(rf.matchIndex[peer])
		rf.mu.Unlock()
		reply := &AppendEntriesReply{}
		DPrintf("[Leader:%v][not empty:appendEntries]:send to %v, logSize: %v,logEntry:%v\n", rf.me, peer, len(args.Entries), args.Entries)
		if ok := rf.sendAppendEntries(peer, args, reply); ok { //net level
			rf.mu.Lock()
			if !reply.Success { //todo: reply.Term > rf.currentTerm, consider other situation
				if reply.Term > rf.currentTerm {
					rf.becomeFollower()
				} else if reply.Term == rf.currentTerm {
					rf.nextIndex[peer]--
					rf.matchIndex[peer]--
				}
			} else { //append entries success
				rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1
				rf.advanceCommitIndexForLeader()
			}
			rf.mu.Unlock()

		} else {
			DPrintf("[sendAppendEntries][fail],fail log:%v\n", args)
		}
	}

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Term: %v || Start() called\n", rf.currentTerm)
	if rf.role != Leader {
		return -1, -1, false
	}
	DPrintf("[Leader:%v], get command:%v\n", rf.me, command)
	index, term = rf.getLastLog().Index+1, rf.currentTerm
	rf.logEntries = append(rf.logEntries, LogEntry{
		Term:    term,
		Command: command,
		Index:   index,
	})
	rf.persist()
	rf.matchIndex[rf.me], rf.nextIndex[rf.me] = index, index+1
	for peer := range rf.peers {
		if peer != rf.me {
			DPrintf("[Leader:%v]: Signal a peer:%v, match[peer]:%v\n", rf.me, peer, rf.matchIndex[peer])
			rf.replicatorCond[peer].Signal()
		}
	}
	// for rf.commitIndex < index {
	// 	rf.applyCond.Wait()
	// }
	// return not wait most of peer append the log
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		rf.mu.Lock()
		heartbtTime := rf.heartbeatTime
		role := rf.role
		rf.mu.Unlock()

		if role != Leader && time.Since(heartbtTime) > 400*time.Millisecond {
			rf.startElectionWithLock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElectionWithLock() {
	rf.mu.Lock()
	rf.becomeCandidate()
	DPrintf("[follower==>candidate:%v]:Term: %v\n", rf.me, rf.currentTerm)
	rf.mu.Unlock()
	for i := 0; i < rf.peersCnt; i++ {
		if rf.me != i {
			go rf.sendRequestVoteToOneWithLock(i)
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh
	initARaft(rf, persister)

	DPrintf("[after Init]: log[%v]\n", rf.logEntries)
	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier() //start applier goroutine
	return rf
}

func initARaft(rf *Raft, persister *tester.Persister) {
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logEntries = make([]LogEntry, 1)

	rf.heartbeatTime = time.Now()
	rf.peersCnt = len(rf.peers)
	rf.role = Follower

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, rf.peersCnt)

	rf.matchIndex = make([]int, rf.peersCnt)

	rf.replicatorCond = make([]*sync.Cond, rf.peersCnt)

	rf.readPersist(persister.ReadRaftState())

	rf.applyCond = sync.NewCond(&rf.mu)

	for peer := range rf.peers {
		rf.matchIndex[peer], rf.nextIndex[peer] = 0, 1
		if peer != rf.me {
			rf.replicatorCond[peer] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(peer)
		}
	}
	go rf.maintainHeartBeatWithLock()
}

func (rf *Raft) maintainHeartBeatWithLock() {
	for !rf.killed() {
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()
		role := rf.role
		peersCnt := rf.peersCnt
		rf.mu.Unlock()
		if role == Leader { //only Leader can send heartbeat
			for i := range peersCnt {
				if i != rf.me {
					//todo: send 1.empty AppendEntries or 2.AppendEntries with log
					// go rf.sendEmptyAppendEntriesToOneWithLock(i)
					DPrintf("[maintainHeartBeat] to peer[%v]", i)
					go rf.sendAppendEntriesToOneWithLock(i)
				}
			}
		}
	}

}

func (rf *Raft) getFirstLog() LogEntry {
	return rf.logEntries[0]
}

func (rf *Raft) getLastLog() LogEntry {
	return rf.logEntries[len(rf.logEntries)-1]
}

func (rf *Raft) genRequestVoteArgs() *RequestVoteArgs {
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm:  rf.getLastLog().Term,
	}
}

func (rf *Raft) genAppendEntriesArgs(preLogIndex int) *AppendEntriesArgs {
	firstLogIndex := rf.getFirstLog().Index
	entries := make([]LogEntry, len(rf.logEntries[preLogIndex-firstLogIndex+1:]))
	copy(entries, rf.logEntries[preLogIndex-firstLogIndex+1:])
	return &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: preLogIndex,
		PrevLogTerm:  rf.logEntries[preLogIndex-firstLogIndex].Term,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
}

func (rf *Raft) isLogMatched(index, term int) bool {
	return index <= rf.getLastLog().Index && term == rf.logEntries[index-rf.getFirstLog().Index].Term //valid index check？
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()

		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
			DPrintf("[follower:%v][commit log index]: lastApplied==>%v commitIndex==>%v\n", rf.me, rf.lastApplied, rf.commitIndex)
		}

		firstLogIndex, commitIndex, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.logEntries[lastApplied-firstLogIndex+1:commitIndex-firstLogIndex+1])
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}

		}
		rf.mu.Lock()
		rf.lastApplied = commitIndex
		rf.mu.Unlock()
	}
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	for !rf.killed() {
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		rf.sendAppendEntriesToOneWithLock(peer)
		DPrintf("[peer:%v] send append entries to %v end\n", rf.me, peer)
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// check the logs of peer is behind the leader
	DPrintf("[leader(%v) match:%v]peer(%v) match index: %v, leader lastLog:%v\n", rf.me, rf.role == Leader && rf.matchIndex[peer] < rf.getLastLog().Index, peer, rf.matchIndex[peer], rf.getLastLog().Index)
	return rf.role == Leader && rf.matchIndex[peer] < rf.getLastLog().Index
}

func (rf *Raft) advanceCommitIndexForLeader() {
	n := len(rf.matchIndex)
	sortMatchIndex := make([]int, n)
	copy(sortMatchIndex, rf.matchIndex)
	sort.Ints(sortMatchIndex)
	newCommitIndex := sortMatchIndex[n-(n/2+1)]
	if newCommitIndex > rf.commitIndex {
		if rf.isLogMatched(newCommitIndex, rf.currentTerm) {
			DPrintf("{Node %v} advances commitIndex from %v to %v in term %v\n", rf.me, rf.commitIndex, newCommitIndex, rf.currentTerm)
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		}
	}
}

func shrinkEntries(entries []LogEntry) []LogEntry {
	const lenMultiple = 2
	if cap(entries) > len(entries)*lenMultiple {
		newEntries := make([]LogEntry, len(entries))
		copy(newEntries, entries)
		return newEntries
	}
	return entries
}

// =================================================
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	// unused fields
	// Offset int	// byte offset where chunk is positioned in the snapshot file
	// Done   bool	// true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) genInstallSnapshotArgs() *InstallSnapshotArgs {
	firstLog := rf.getFirstLog()
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: firstLog.Index,
		LastIncludedTerm:  firstLog.Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	return args
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v, term %v}} after processing InstallSnapshot,  InstallSnapshotArgs %v and InstallSnapshotReply %v ", rf.me, rf.role, rf.currentTerm, args, reply)

	reply.Term = rf.currentTerm

	// reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}
	rf.becomeFollower()
	rf.heartbeatTime = time.Now()
	// rf.electionTimer.Reset(RandomElectionTimeout())

	// check the snapshot is more up-to-date than the current log
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}
	rf.logEntries = []LogEntry{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex, Command: nil}}
	rf.persistWithSnapshot(args.Data)
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex

	go func() {
		rf.applyCh <- raftapi.ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
