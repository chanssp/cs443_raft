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
import "labrpc"
import "time"
import "math/rand"
import "fmt"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (Lab1, Lab2, Challenge1).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	
	// Data for Lab1
	currentTerm	int
	votedFor	int
	state 		int 	// 1: leader, 2: follower, 3: candidate	
	voteCount	int
	heartBeatCh	chan bool
	newLeaderCh	chan bool
	grantVoteCh	chan bool

	// Data for Lab2
	log[]		 Log 	// log entries
	commitIndex  int 	// index of highest log entry known to be committed
	lastApplied  int 	// index of highest log entry applied to state machine
	applyLogCh	 chan ApplyMsg

	nextIndex[]  int
	matchIndex[] int

}

// Data type used in Lab 2
type Log struct {
	Term 	int
	Command interface{}
}


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (Lab1).
	term = rf.currentTerm

	if rf.state == 1 {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (Challenge1).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (Challenge1).
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


	// ========================
	// ===Just for debugging===
	// ========================
	// ===Erase before commit==
	// ========================
	fmt.Printf("hello\n")
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (Lab1, Lab2).
	// Data for Lab1
	Term		 int
	CandidateId	 int

	// Data for Lab2
	LastLogIndex int
	LastLogTerm	 int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (Lab1).
	Term 		int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (Lab1, Lab2).
	
	var candTerm 	int
	var candId 	int
	candTerm = args.Term
	candId = args.CandidateId

	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch {
	case candTerm <= rf.currentTerm:
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	case candTerm > rf.currentTerm:
		rf.votedFor = candId
		rf.currentTerm = candTerm
		reply.Term = candTerm
		reply.VoteGranted = true
		rf.state = 2
		rf.grantVoteCh <- reply.VoteGranted
	default:
		// this part of the code is brought from example code for Lab1 from KLMS
		reply.Term = candTerm
		if rf.votedFor == -1 || rf.votedFor == candId && rf.checkLogs(args) {
			reply.VoteGranted = true
			rf.votedFor = candId
		} else {
			reply.VoteGranted = false
		}
	}
}

// this code was brought from example code for Lab1 from KLMS
func (rf *Raft) checkLogs(args *RequestVoteArgs) bool {
	switch compareInt(rf.log[rf.lastApplied].Term, args.LastLogTerm){
	case -1:
		return true
	case 0:
		if rf.lastApplied <= args.LastLogIndex{
			return true
		}
		fallthrough
	default:
		return false
	}
}

func compareInt(a, b int) int {
	if a>b{
		return 1
	}
	if a<b{
		return -1
	}
	return 0
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
	
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.VoteGranted {
		rf.voteCount += 1

		if rf.isMajority(rf.voteCount) {
			rf.newLeaderCh <- true
		}
	} else {
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.state = 2	// becomes a follower
		}
	}

	return ok
}

// 
// Lab1: AppendEntries RPC arguments structure.
// 
type AppendEntriesArgs struct {
	// Data for Lab1
	Term 		int
	LeaderId	int

	// Data for Lab2
	PrevLogIndex int
	PrevLogTerm  int
	Entries[]	 Log
	LeaderCommit int
}

// 
// Lab1: AppendEntries RPC reply structure.
// 
type AppendEntriesReply struct {
	Term 	int
	Success bool
}

// 
// AppendEntries RPC handler 
// Lab1: Used to implement heartbeat
// Lab2: Invoked by leader to replicate log entries
// 
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	var leaderTerm int
	leaderTerm = args.Term

	rf.mu.Lock()
	defer rf.mu.Unlock()

	switch {
	case leaderTerm < rf.currentTerm:
		reply.Term = rf.currentTerm
		reply.Success = false
	case leaderTerm >= rf.currentTerm:
		rf.currentTerm = leaderTerm
		reply.Term = leaderTerm
		reply.Success = true
		rf.state = 2
		rf.heartBeatCh <- reply.Success
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if len(rf.log) > 1 {
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
		} else {
			rf.updateLog(args.PrevLogIndex + 1, args.Entries)
			reply.Success = true

			if args.LeaderCommit > rf.commitIndex {
				lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
				if args.LeaderCommit > lastNewEntryIndex {
					rf.commitIndex = lastNewEntryIndex
				} else {
					rf.commitIndex = args.LeaderCommit
				}
				rf.applyLog()
			}

		}
	} // len(rf.log) > 1
}

// 
// sendAppendEntries callback function
// 
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.state = 2	// becomes a follower
	}

	if rf.LastLogIndex() >= rf.nextIndex[server] {
		if !reply.Success {
			if rf.nextIndex[server] > 1 {
				rf.nextIndex[server] -= 1
			}
		} else {
			rf.nextIndex[server] = rf.nextIndex[server] + len(args.Entries)
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)

			for i := rf.commitIndex + 1; i <= rf.LastLogIndex(); i++ {
				var count int
				count = 1

				for j := 0; j < len(rf.peers); j++ {
					for j != rf.me {
						if rf.matchIndex[j] >= i && rf.log[i].Term == rf.currentTerm {
							count += 1
						}
					}
				}

				if rf.isMajority(count) {
					rf.commitIndex = i
					rf.applyLog()
				}
			}
		}
	}
	
	return ok
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
// if it's ever committed. the secaond return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (Lab2).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term, isLeader = rf.GetState()

	if isLeader {
		var newEntry Log

		newEntry.Term = term
		newEntry.Command = command

		rf.log = append(rf.log, newEntry)
		index = rf.LastLogIndex() + 1
	} 

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
}

// when becomes leader, server must call this function
func (rf *Raft) initLeader() {
	rf.voteCount = 0
	rf.state = 1	// becomes a leader

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.nextIndex[i] = rf.LastLogIndex() + 1
			rf.matchIndex[i] = 0
		}
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

	// Your initialization code here (Lab1, Lab2, Challenge1).
	rf.currentTerm = 0
	rf.votedFor = -1	// null value
	rf.state = 2	// starts as a follower
	rf.voteCount = 0

	rf.heartBeatCh = make(chan bool)
	rf.newLeaderCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyLogCh = applyCh

	// Initialize log with meaningless log entry
	// since log starts from index 1.
	var initLog Log
	initLog.Term = 0
	initLog.Command = 0
	rf.log = append(rf.log, initLog)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	go rf.RunRaftNode()

	return rf
}

// 
// Lab1: this function run the server continuously
// 
func (rf *Raft) RunRaftNode() {
	var state int

	for {
		state = rf.state
		// fmt.Printf("id: %d, term: %d\n", rf.me, rf.currentTerm)

		switch state{
			case 1:	// leader
				var aeArgs *AppendEntriesArgs
				aeArgs = &AppendEntriesArgs{}
				
				rf.mu.Lock()
				aeArgs.Term = rf.currentTerm
				aeArgs.LeaderId = rf.me
				aeArgs.LeaderCommit = rf.commitIndex
				rf.mu.Unlock()

				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						aeArgs.PrevLogIndex = rf.nextIndex[i] - 1
						aeArgs.PrevLogTerm = rf.log[aeArgs.PrevLogIndex].Term
						aeArgs.Entries = rf.log[rf.nextIndex[i]:]

						go rf.sendAppendEntries(i, aeArgs, &AppendEntriesReply{})
					}
				}

				time.Sleep(200 * time.Millisecond)
			case 2: // follower
				select {
				case <- rf.heartBeatCh:
					rf.state = 2
				case <- rf.grantVoteCh:
					rf.state = 2
				case <- time.After(time.Duration(rand.Intn(200) + 400) * time.Millisecond):
					// election timeout passed
					rf.state = 3
				}
			case 3: // candidate
				var rvArgs *RequestVoteArgs
				rvArgs = &RequestVoteArgs{}
				
				rf.mu.Lock()
				rf.currentTerm += 1
				rf.votedFor = rf.me
				rf.voteCount += 1

				rvArgs.Term = rf.currentTerm
				rvArgs.CandidateId = rf.me
				rvArgs.LastLogIndex = rf.LastLogIndex()
				rvArgs.LastLogTerm = rf.log[rf.LastLogIndex()].Term
				rf.mu.Unlock()
				
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						go rf.sendRequestVote(i, rvArgs, &RequestVoteReply{})
					}
				}

				select {
				case <- rf.newLeaderCh:
					rf.initLeader()
				case <- rf.heartBeatCh:
					rf.voteCount = 0
					rf.state = 2	// becomes a follower
				case <- time.After(time.Duration(rand.Intn(200) + 800) * time.Millisecond):
					// no leader has been elected in this term.
					// this rf starts next term without changing state
				}
		}
	}
}


// ==============================================
// ============== Helper functions ============== 
// ==============================================
func (rf *Raft) isMajority(what int) bool {
	numPeers := len(rf.peers) / 2

	if what > numPeers {
		return true
	} else {
		return false
	}
}

func (rf *Raft) LastLogIndex() int  {
	return len(rf.log) - 1
}

// delete logs from startIndex and append new logs from argument
func (rf *Raft) updateLog(startIndex int, newEntries []Log) {
	rf.log = append(rf.log[:startIndex], newEntries...)
}

// If commitIndex > lastApplied: increment lastApplied, 
// apply log[lastApplied] to state machine (§5.3) 
func (rf *Raft) applyLog() {
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied += 1
		
		var message ApplyMsg

		message.CommandValid = true
		message.Command = rf.log[rf.lastApplied].Command
		message.CommandIndex = rf.lastApplied

		rf.applyLogCh <- message
	}

	rf.lastApplied = rf.commitIndex
}

