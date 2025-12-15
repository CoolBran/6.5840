package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type LogEntry struct {
	Term    int
	Index   int //first index is 1
	Content string
}

type raftRole int

const (
	follower raftRole = iota
	candidate
	leader
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
	voteReqFinish chan struct{}
	peersCnt      int
	role          raftRole
	vote2Me       map[int]bool //vote for me todo
	commitIndex   int          //index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied   int          //index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// //volatile state on leaders
	nextIndex  map[int]int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex map[int]int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	//consider the caller（use mu or not）todo
	term = rf.currentTerm
	isleader = rf.votedFor == rf.me
	return term, isleader
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
	Voter       int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//todo

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	reply.Voter = rf.me

	//term smaller than me
	if args.Term < rf.currentTerm {
		return
	}

	//already vote
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}

	//log older than me
	if rf.checkLogOlderMe(args.LastLogTerm, args.LastLogIndex) {
		return
	}

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true

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

}

func (rf *Raft) checkLogOlderMe(lastLogTerm int, lastLogIndex int) bool {
	if lastLogTerm < rf.logEntries[len(rf.logEntries)-1].Term {
		return true
	}
	if lastLogIndex < rf.logEntries[len(rf.logEntries)-1].Index {
		return true
	}
	return false
}

func (rf *Raft) sendRequestVoteToAll(index int) {

	logSize := len(rf.logEntries)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.logEntries[logSize-1].Index,
		LastLogTerm:  rf.logEntries[logSize-1].Term,
	}
	reply := RequestVoteReply{}
	ok := false
	for i := 0; i < 3; i++ {
		if ok = rf.sendRequestVote(index, &args, &reply); ok {
			rf.vote2Me[reply.Voter] = false
			if reply.VoteGranted {
				rf.vote2Me[reply.Voter] = true
			}
			if len(rf.vote2Me) == rf.peersCnt {
				rf.voteReqFinish <- struct{}{}
			}
			return
		}
	}
	rf.vote2Me[reply.Voter] = false
	if len(rf.vote2Me) == rf.peersCnt {
		rf.voteReqFinish <- struct{}{}
	}
}

func (rf *Raft) sendEmptyAppendEntriesToAll(index int) {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: rf.logEntries[len(rf.logEntries)-1].Index,
		PrevLogTerm:  rf.logEntries[len(rf.logEntries)-1].Term,
		// Entries:      rf.logEntries[len(rf.logEntries)-1:],
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	ok := false
	if ok = rf.sendAppendEntries(index, &args, &reply); ok {
		rf.matchIndex[index] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[index] = rf.matchIndex[index] + 1
	}
}

func (rf *Raft) ifBeLeader() bool {
	voteCnt := 0
	for _, vote := range rf.vote2Me {
		if vote {
			voteCnt++
		}
	}
	return voteCnt > len(rf.peers)/2
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
	for rf.killed() == false {

		// Your code here (3A)

		if time.Since(rf.heartbeatTime) > 400*time.Millisecond {
			rf.role = candidate
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.vote2Me = make(map[int]bool)
			rf.vote2Me[rf.me] = true
			for i := 0; i < rf.peersCnt; i++ {
				// logSize := len(rf.logEntries)
				// args := RequestVoteArgs{
				// 	Term:         rf.currentTerm,
				// 	CandidateId:  rf.me,
				// 	LastLogIndex: rf.logEntries[logSize-1].Index,
				// 	LastLogTerm:  rf.logEntries[logSize-1].Term,
				// }
				// reply := RequestVoteReply{}
				// //use gorutine here todo
				// if ok := rf.sendRequestVote(i, &args, &reply); ok {
				// 	if reply.VoteGranted {
				// 		rf.vote2Me[reply.Voter] = true
				// 	} else {
				// 		rf.vote2Me[reply.Voter] = false
				// 	}
				// }
				go rf.sendRequestVoteToAll(i)
			}
			//check if be a leader: sendRequestVoteToAll 流程结束判断（todo：有待商榷）
			<-rf.voteReqFinish
			if rf.ifBeLeader() {
				rf.role = leader
				for i := 0; i < rf.peersCnt; i++ {
					go rf.sendEmptyAppendEntriesToAll(i)
				}
			}
		}

		//todo: what time
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logEntries = make([]LogEntry, 0, 0)
	rf.peersCnt = len(peers)
	rf.role = follower
	rf.vote2Me = make(map[int]bool)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
