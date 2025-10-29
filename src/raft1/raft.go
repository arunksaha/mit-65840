package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Term int

type ServerId int

const ServerIdNull = -1

type Index int

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	// mu        sync.Mutex          // Lock to protect shared access to this peer's state
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Latest term server has seen (initialized to 0 on first boot, increases monotonically)
	currentTerm Term

	// candidateId that received vote in current term
	votedFor ServerId

	// current state
	state State

	// Time instant when the last heartbeat was received
	lastHeard time.Time

	// Servers may their own random election timeout, chosen at initialization.
	electionTimeout time.Duration

	// All servers should have same heartbeat interval chosen at iniitialization.
	heartbeatInterval time.Duration
}

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	term = int(rf.currentTerm)
	isleader = rf.state == Leader
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
	// Candidate's Term
	Term Term

	// Server requesting vote
	CandidateId ServerId
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	// Current term of responder, for candidate to update itself
	CurrentTerm Term

	// True means candidate received vote
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()

	// Unless explicitly set below, vote is not granted :-(
	reply.VoteGranted = false

	if rf.currentTerm < args.Term {
		// This server is at older term, move to newer term as a follower
		rf.transitionToFollower(args.Term)
	}

	// Note: The following equality might be thanks to the above transition.
	if rf.currentTerm == args.Term {
		// Grant vote iff not voted yet or (already) voted for this candidate
		votedForOk := rf.votedFor == ServerIdNull || rf.votedFor == args.CandidateId
		if votedForOk {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true

			// When a vote is granted, count it as hearing from the Leader;
			// otherwise, the election timer might trigger unnecessarily.
			rf.lastHeard = time.Now()
		}
	}

	// Always respond with the latest value of this server's current term
	reply.CurrentTerm = rf.currentTerm

	rf.mu.Unlock()

	mesg := fmt.Sprintf("RequestVote done: request=%+v, reply=%+v", *args, *reply)
	rf.DLogState(mesg)
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
	DLog("RequestVote %d --> %d: request=%+v\n", rf.me, server, *args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		DLog("RequestVote %d <-- %d: reply=%+v\n", rf.me, server, *reply)
	}
	return ok
}

type AppendEntriesArgs struct {
	Term     Term     // Leader's term
	LeaderId ServerId // Leader's id
}

type AppendEntriesReply struct {
	Term    Term // Follower's current term
	Success bool // True if follower has same or lesser term
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	termIncremented := false

	rf.mu.Lock()

	// Always reply with the current term.
	reply.Term = rf.currentTerm

	// Reply false if this follower is at a higher term.
	if rf.currentTerm > args.Term {
		reply.Success = false
	} else {
		// If this server is at a lower term, update it.
		if rf.currentTerm < args.Term {
			rf.transitionToFollower(args.Term)
			termIncremented = true
		}

		// Reset election timer.
		rf.lastHeard = time.Now()

		// TODO 3B Log consistency checks

		reply.Success = true
	}

	rf.mu.Unlock()

	mesg := fmt.Sprintf("AppendEntries from %d completed, term incremented? = %t", args.LeaderId, termIncremented)
	rf.DLogState(mesg)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DLog("AppendEntries %d --> %d: request=%+v\n", rf.me, server, *args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		DLog("AppendEntries %d <-- %d reply=%+v\n", rf.me, server, *reply)
	}
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
	rf.mu.Lock()
	rf.raftInit()
	rf.mu.Unlock()

	mesg := "Server Killed"
	rf.DLogState(mesg)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		var lastHeard time.Time
		rf.mu.RLock()
		lastHeard = rf.lastHeard
		timeElapsed := time.Since(rf.lastHeard)
		heartbeatIsRecent := timeElapsed < rf.electionTimeout
		electionNecessary := (rf.state != Leader) && !heartbeatIsRecent
		rf.mu.RUnlock()

		if electionNecessary {
			mesg := fmt.Sprintf("trigger ELECTION (lastHeard = %s, timeElapsed = %s)", lastHeard, timeElapsed)
			rf.DLogState(mesg)
			go rf.startElection()
		}

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
	rf.raftInit()

	DLog("Server %d: Created Raft with %d peers, election timeout = %v, heartbeat interval = %v\n",
		rf.me, len(peers), rf.electionTimeout, rf.heartbeatInterval)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) raftInit() {
	rf.currentTerm = 0
	rf.state = Follower
	rf.resetElectionTimeout()
	rf.heartbeatInterval = 100 * time.Millisecond
	rf.votedFor = ServerIdNull
	rf.lastHeard = time.Now()
}

func (rf *Raft) startElection() {
	var curState State
	var curTerm Term
	var npeers int
	var quorum int

	rf.mu.Lock()
	rf.state = Candidate
	curState = rf.state
	rf.currentTerm = rf.currentTerm + 1
	curTerm = rf.currentTerm
	npeers = len(rf.peers)
	quorum = len(rf.peers)/2 + 1
	rf.votedFor = ServerId(rf.me)
	rf.lastHeard = time.Now()
	rf.mu.Unlock()

	DLog("Server %d: Starting ELECTION term (incremented) = %d, state = %d peers = %d, quorum = %d\n",
		rf.me, curTerm, curState, npeers, quorum)

	votes := int32(1)
	var wg sync.WaitGroup
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		wg.Add(1)
		go func(server int) {
			defer wg.Done()
			args := RequestVoteArgs{Term: curTerm, CandidateId: ServerId(rf.me)}
			var reply RequestVoteReply
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				ignoreReply := rf.state != Candidate
				if ignoreReply {
					DLog("Server %d RequestVote reply ignored term=%d, state=%d\n", rf.me, rf.currentTerm, rf.state)
				} else {
					shouldBeFollower := rf.currentTerm < reply.CurrentTerm
					incrementVotes := reply.VoteGranted && rf.state == Candidate
					if shouldBeFollower {
						rf.transitionToFollower(reply.CurrentTerm)
					} else if incrementVotes {
						atomic.AddInt32(&votes, 1)
						curVotes := int(atomic.LoadInt32(&votes))
						DLog("Server %d Ongoing Election, received vote from %d, current votes = %d\n",
							rf.me, server, curVotes)
						if curVotes >= quorum {
							rf.state = Leader
							rf.lastHeard = time.Now()
							DLog("Server %d End ELECTION this now LEADER (current votes = %d) at term = %d\n",
								rf.me, curVotes, rf.currentTerm)
							go rf.sendHeartbeats()
						}
					}
				}
			}
		}(idx)
	}
	wg.Wait()
}

// sendHeartbeats() is used only by the leader to send heartbeat to all other servers.
func (rf *Raft) sendHeartbeats() {
	rf.DLogState("Begin heartbeats")
	for {
		rf.mu.RLock()
		stop := rf.state != Leader || rf.killed()
		if stop {
			rf.mu.RUnlock()
			break
		}
		term := rf.currentTerm
		heartbeatInterval := rf.heartbeatInterval
		rf.mu.RUnlock()

		for idx := range rf.peers {
			if idx == rf.me {
				continue
			}
			go func(server int) {
				args := AppendEntriesArgs{Term: term, LeaderId: ServerId(rf.me)}
				var reply AppendEntriesReply
				ok := rf.sendAppendEntries(server, &args, &reply)
				if ok {
					shouldBeFollower := term < reply.Term
					if shouldBeFollower {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						rf.transitionToFollower(reply.Term)
					}
				}
			}(idx)
		}
		time.Sleep(heartbeatInterval)
	}
	rf.DLogState("End heartbeats")
}

func (rf *Raft) transitionToFollower(newTerm Term) {
	rf.state = Follower
	rf.votedFor = ServerIdNull
	rf.currentTerm = newTerm
	rf.lastHeard = time.Now()
	rf.resetElectionTimeout()
	DLog("Server %d: transition to Follower: state = %d term = %d, voted for = %d\n",
		rf.me, rf.state, rf.currentTerm, rf.votedFor)
}

const electionTimeoutMin = 300 // Milliseconds
const electionTimeoutMax = 500

func (rf *Raft) resetElectionTimeout() {
	electionTimeoutInMs := getRandomValueInRange(electionTimeoutMin, electionTimeoutMax)
	rf.electionTimeout = time.Duration(electionTimeoutInMs) * time.Millisecond
	DLog("Server %d: term=%d, state=%d electionTimeout set to %v\n",
		rf.me, rf.currentTerm, rf.state, rf.electionTimeout)
}

func (rf *Raft) DLogState(mesg string) {
	var serverId int
	var state int
	var term int
	var electionTimeout time.Duration
	var elapsed time.Duration
	var votedFor ServerId

	rf.mu.RLock()
	serverId = rf.me
	state = int(rf.state)
	term = int(rf.currentTerm)
	votedFor = rf.votedFor
	electionTimeout = rf.electionTimeout
	elapsed = time.Since(rf.lastHeard)
	rf.mu.RUnlock()

	DLog("Server %d term=%d, state=%d votedFor=%d, election timeout = %v, elapsed = %v: %s\n",
		serverId, term, state, votedFor, electionTimeout, elapsed, mesg)
}

func assert(cond bool, mesg string) {
	if !cond {
		log.Println(mesg)
		panic("assertion failed")
	}
}

// Choose a random number in the range [lo, hi]
func getRandomValueInRange(lo int, hi int) int {
	if hi < lo {
		panic("invalid range: lo must lower than hi")
	}
	return lo + rand.Intn(hi-lo)
}
