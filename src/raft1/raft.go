package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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

// logEntry are created in handling of Start() and appended to rf.log.
type logEntry struct {
	Term    int         // term when the entry was received by leader
	Command interface{} // client command to apply
}

// A Go object implementing a single Raft peer.
type Raft struct {
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

	// Log entries; each entry contains command for state machine,
	// and term when entry was received by leader (first index is 1)
	log []logEntry

	// Index of highest log entry known to have been committed
	// (not just accepted) to the Raft cluster.
	commitIndex int

	// Index of highest log entry applied to state machine
	lastApplied int

	// -- The followiing state are maintained at leaders and reinitialized after election --

	// for each server, index of the next log entry to send there
	nextIndex []int

	// for each server, index of highest log entry to be replicated there
	matchingIndex []int
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
	rf.readLock()
	defer rf.readUnlock()
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

	rf.persistImpl(true)
}

func (rf *Raft) persistImpl(grabReadLock bool) {
	encodeOk := true

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if grabReadLock {
		rf.mu.RLock()
	}

	if err := e.Encode(rf.currentTerm); err != nil {
		log.Printf("Server %d: ERROR encoding currentTerm: %v\n", rf.me, err)
		encodeOk = false
	}
	if err := e.Encode(rf.votedFor); err != nil {
		log.Printf("Server %d: ERROR encoding votedFor: %v\n", rf.me, err)
		encodeOk = false
	}
	if err := e.Encode(rf.log); err != nil {
		log.Printf("Server %d: ERROR encoding log: %v\n", rf.me, err)
		encodeOk = false
	}

	if grabReadLock {
		rf.mu.RUnlock()
	}

	if encodeOk {
		raftstate := w.Bytes()
		rf.persister.Save(raftstate, nil)
	}
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
	var rfTerm int
	var rfVotedFor int
	var rfLog []logEntry
	if err := d.Decode(&rfTerm); err != nil {
		log.Printf("Server %d: ERROR decoding currentTerm %v\n", rf.me, err)
		return
	}
	if err := d.Decode(&rfVotedFor); err != nil {
		log.Printf("Server %d: ERROR decoding votedFor %v\n", rf.me, err)
		return
	}
	if err := d.Decode(&rfLog); err != nil {
		log.Printf("Server %d: ERROR decoding log %v\n", rf.me, err)
		return
	}

	rf.mu.Lock()
	rf.currentTerm = Term(rfTerm)
	rf.votedFor = ServerId(rfVotedFor)
	rf.log = rfLog
	rf.mu.Unlock()

	assert(len(rf.log) >= 1, "rf.log must have at least 1 entry")
	assert(rf.log[0].Term == 0, "rf.log[0] must be Term 0")
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.lock()
	defer rf.unlock()
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

	// Server requesting vote (the candidate server id)
	CandidateId ServerId

	// LastLogIndex is the index of candidate's last log entry
	LastLogIndex int

	// LastLogTerm is the term of candidate's last log entry
	LastLogTerm int
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
	rf.lock()

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
		lastLogIndex := len(rf.log) - 1
		lastLogTerm := rf.log[lastLogIndex].Term
		candidateUpToDate := (lastLogTerm < args.LastLogTerm) ||
			((lastLogTerm == args.LastLogTerm) && (lastLogIndex <= args.LastLogIndex))
		if votedForOk && candidateUpToDate {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true

			// When a vote is granted, count it as hearing from the Leader;
			// otherwise, the election timer might trigger unnecessarily.
			rf.lastHeard = time.Now()
		}
	}

	// Always respond with the latest value of this server's current term
	reply.CurrentTerm = rf.currentTerm

	rf.unlock()

	rf.persist()

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

// AppendEntries used for both (a) heartbeat, and (b) log (rf.log) replication.
type AppendEntriesArgs struct {
	Term     Term     // Leader's term
	LeaderId ServerId // Leader's id

	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of PrevLogIndex
	Entries      []logEntry // log entries to store
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    Term // Follower's current term
	Success bool // True if follower has same or lesser term

	ConflictTerm  Term  // -1 if not used
	ConflictIndex Index // first index of ConflictTerm in follower (or len(log) if follower is shorter)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	termIncremented := false

	rf.lock()

	// Reset election timer
	rf.lastHeard = time.Now()

	// Default reply
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = 0

	// 1) Leader is at a lower term, reject
	if rf.currentTerm > args.Term {
		mesg := fmt.Sprintf("AppendEntries from %d rejected, currentTerm=%d > args.Term=%d",
			args.LeaderId, rf.currentTerm, args.Term)
		DLog("Server %d: %s\n", rf.me, mesg)
		return
	}

	// 2) Leader is at higher term, step down to follower
	if rf.currentTerm < args.Term {
		rf.transitionToFollower(args.Term)
		termIncremented = true
	}

	// 3) Log consistency check:
	// Does follower have an entry at PrevLogIndex whose term matches PrevLogTerm?
	rfLogLen := len(rf.log)
	// logs with sentinel at index 0 => highest index = rfLogLen - 1
	if args.PrevLogIndex > rfLogLen-1 {
		// Follower log is too short, tell leader to advance to rfLogLen.
		reply.ConflictTerm = -1
		reply.ConflictIndex = Index(rfLogLen) // Leader should next try at this index
		mesg := fmt.Sprintf("AppendEntries from %d rejected, log too short: PrevLogIndex=%d, rfLogLen=%d",
			args.LeaderId, args.PrevLogIndex, rfLogLen)
		DLog("Server %d: %s\n", rf.me, mesg)
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Follower log term mismatch
		conflictTerm := rf.log[args.PrevLogIndex].Term
		// Find first index with conflicting term
		firstIdx := args.PrevLogIndex
		for firstIdx > 0 && rf.log[firstIdx-1].Term == conflictTerm {
			firstIdx--
		}
		reply.ConflictTerm = Term(conflictTerm)
		reply.ConflictIndex = Index(firstIdx)
		return
	}

	// 4) At this point prev log matches - accept incoming entries:
	// iterate entries and overwrite conflicting entries or append new ones
	insertIdx := args.PrevLogIndex + 1
	i := 0
	for ; i < len(args.Entries); i++ {
		rfIdx := insertIdx + i
		if rfIdx < len(rf.log) {
			// If mismatch, truncate follower log from here and append remainder
			if rf.log[rfIdx].Term != args.Entries[i].Term {
				rf.log = rf.log[:rfIdx]
				break
			}
			// else same entry: continue
		} else {
			break
		}
	}
	// Append remaining entries
	for ; i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i])
	}

	// 5) Update commitIndex: leaderCommit means:
	// set commitIndex = min(leaderCommit, index of last new entry)
	lastIndex := len(rf.log) - 1
	if rf.commitIndex < args.LeaderCommit {
		rf.commitIndex = min(args.LeaderCommit, lastIndex)
	}

	reply.Success = true
	reply.ConflictTerm = -1
	reply.ConflictIndex = 0

	rf.unlock()

	rf.persist()

	mesg := fmt.Sprintf("AppendEntries from %d completed, term incremented? = %t",
		args.LeaderId, termIncremented)
	DLog("Server %d: %s\n", rf.me, mesg)
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
	rf.lock()

	if rf.state != Leader {
		return -1, int(rf.currentTerm), false
	}

	term = int(rf.currentTerm)

	entry := logEntry{
		Term:    term,
		Command: command,
	}

	rf.log = append(rf.log, entry)
	index = len(rf.log) - 1

	rf.nextIndex[rf.me] = index + 1
	rf.matchingIndex[rf.me] = index

	rf.unlock()

	rf.persist()

	mesg := fmt.Sprintf("Server %d: Start(): index=%d, term=%d, isLeader?=%t\n",
		rf.me, index, term, isLeader)
	DLog("%s\n", mesg)

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
	rf.lock()
	rf.raftInit()
	rf.unlock()

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
		rf.readLock()
		lastHeard = rf.lastHeard
		timeElapsed := time.Since(rf.lastHeard)
		heartbeatIsRecent := timeElapsed < rf.electionTimeout
		electionNecessary := (rf.state != Leader) && !heartbeatIsRecent
		rf.readUnlock()

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

	go rf.applier(applyCh)

	return rf
}

func (rf *Raft) raftInit() {
	rf.currentTerm = 0
	rf.state = Follower
	rf.resetElectionTimeout()
	rf.heartbeatInterval = 100 * time.Millisecond
	rf.votedFor = ServerIdNull
	rf.lastHeard = time.Now()

	rf.log = make([]logEntry, 0)
	zeroEntry := logEntry{
		Term: 0,
	}
	rf.log = append(rf.log, zeroEntry)
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.persist()
}

func (rf *Raft) startElection() {
	var curState State
	var curTerm Term
	var npeers int
	var quorum int

	rf.lock()
	rf.state = Candidate
	curState = rf.state
	rf.currentTerm = rf.currentTerm + 1
	curTerm = rf.currentTerm
	npeers = len(rf.peers)
	quorum = len(rf.peers)/2 + 1
	rf.votedFor = ServerId(rf.me)
	rf.lastHeard = time.Now()
	rf.unlock()

	rf.persist()

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
			rf.readLock()
			lastIndex := len(rf.log) - 1
			lastTerm := rf.log[lastIndex].Term
			rf.readUnlock()
			args := RequestVoteArgs{
				Term:         curTerm,
				CandidateId:  ServerId(rf.me),
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}
			var reply RequestVoteReply
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				startHeartBeatsGoRoutine := false
				rf.lock()
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
						quorumSatisfied := curVotes >= quorum
						transitionToLeader := rf.state != Leader
						DLog("Server %d Ongoing Election, received vote from %d, current votes = %d (quorum needs %d), quorumSatisfied = %t, transitionToLeader = %t\n",
							rf.me, server, curVotes, quorum, quorumSatisfied, transitionToLeader)
						if quorumSatisfied {
							if transitionToLeader {
								rf.state = Leader
								rf.leaderInit()
								rf.lastHeard = time.Now()
								DLog("Server %d End ELECTION this now LEADER (current votes = %d) at term = %d\n",
									rf.me, curVotes, rf.currentTerm)
								startHeartBeatsGoRoutine = true
							} else {
								DLog("Server %d is already leader\n", rf.me)
							}
						}
					}
				}
				if startHeartBeatsGoRoutine {
					go rf.sendHeartbeats()
				}
				rf.unlock()
			} // if ok
		}(idx)
	}
	wg.Wait()
}

// sendHeartbeats() is used only by the leader to send heartbeat to all other servers.
func (rf *Raft) sendHeartbeats() {
	DLog("Server %d: sendHeartbeats begin\n", rf.me)
	heartbeatInterval := rf.heartbeatInterval
	for {
		DTrace("Server %d: sendHeartbeats outer loop infinite\n", rf.me)
		// Stop if lost leadership
		rf.readLock()
		stop := rf.state != Leader || rf.killed()
		if stop {
			rf.readUnlock()
			break
		}
		term := rf.currentTerm
		rf.readUnlock()
		DTrace("Server %d: sendHeartbeats checked state, killed and updated term = %d\n", rf.me, term)

		for peer := range rf.peers {
			DTrace("Server %d: sendHeartbeats inner loop rf.peers, peer = %d\n", rf.me, peer)
			if peer == rf.me {
				continue
			}
			go func(server int) {
				// Read necessary state under read-lock
				rf.lock()
				next := rf.nextIndex[server]
				prevLogIndex := next - 1
				prevLogTerm := 0
				if 0 <= prevLogIndex && prevLogIndex < len(rf.log) {
					prevLogTerm = rf.log[prevLogIndex].Term
				}
				// Create entries with the remaining entries.
				entries := make([]logEntry, 0, len(rf.log)-next)
				if next < len(rf.log) {
					entries = append(entries, rf.log[next:]...)
				}
				leaderCommit := rf.commitIndex
				rf.unlock()

				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     ServerId(rf.me),
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      entries,
					LeaderCommit: leaderCommit,
				}

				var reply AppendEntriesReply
				ok := rf.sendAppendEntries(server, &args, &reply)
				if !ok {
					return
				}

				rf.lock()
				defer rf.unlock()
				// If follower term larger -> step down
				if reply.Term > rf.currentTerm {
					rf.transitionToFollower(reply.Term)
					return
				}
				// If reply is success, advance nextIndex and matchIndex
				if reply.Success {
					newNextIdx := args.PrevLogIndex + 1 + len(args.Entries)
					if newNextIdx > rf.nextIndex[server] {
						rf.nextIndex[server] = newNextIdx
						rf.matchingIndex[server] = newNextIdx - 1
					}
					return
				}

				// If failed, use conflict optimization
				if reply.ConflictTerm != -1 {
					// find rightmost index in leader's log with ConflictTerm
					idx := -1
					for i := len(rf.log) - 1; i >= 0; i-- {
						if rf.log[i].Term == int(reply.ConflictTerm) {
							idx = i
							break
						}
					}
					if idx >= 0 {
						rf.nextIndex[server] = idx + 1
					} else {
						rf.nextIndex[server] = int(reply.ConflictIndex)
					}
				} else {
					// Fallback: follower shorter
					rf.nextIndex[server] = int(reply.ConflictIndex)
				}
			}(peer)
		}

		// After launching heartbeats, attempt to advance commitIndex
		rf.advanceCommitIndexIfPossible()

		DTrace("Server %d: Sleep %v begin\n", rf.me, heartbeatInterval)
		time.Sleep(heartbeatInterval)
		DTrace("Server %d: Sleep %v end\n", rf.me, heartbeatInterval)
	}
	rf.DLogState("End heartbeats")
}

func (rf *Raft) advanceCommitIndexIfPossible() {
	DTrace("Server %d: advanceCommitIndexIfPossible\n", rf.me)

	rf.lock()
	defer rf.unlock()

	// Last log index
	N := len(rf.log) - 1
	quorum := len(rf.peers)/2 + 1

	for n := rf.commitIndex + 1; n <= N; n++ {
		// Only consier entries in current term
		if rf.log[n].Term != int(rf.currentTerm) {
			continue
		}

		count := 1 // Leader itself
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchingIndex[i] >= n {
				count++
			}
		}
		if count >= quorum {
			rf.commitIndex = n
			// Keep scanning forward
		}
	}
}

// transitionToFollower() must be called while holding rf.lock().
func (rf *Raft) transitionToFollower(newTerm Term) {
	rf.state = Follower
	rf.votedFor = ServerIdNull
	rf.currentTerm = newTerm
	rf.lastHeard = time.Now()
	rf.resetElectionTimeout()
	DLog("Server %d: transition to Follower: state = %d term = %d, voted for = %d\n",
		rf.me, rf.state, rf.currentTerm, rf.votedFor)

	// Persist without grabbing read lock (grabReadLock)
	// since the caller (of transitionToFollower) holds the lock.
	rf.persistImpl(false)
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

	rf.readLock()
	serverId = rf.me
	state = int(rf.state)
	term = int(rf.currentTerm)
	votedFor = rf.votedFor
	electionTimeout = rf.electionTimeout
	elapsed = time.Since(rf.lastHeard)
	nextIndex := rf.nextIndex[:]
	matchingIndex := rf.nextIndex[:]
	rf.readUnlock()

	DLog("Server %d term=%d, state=%d votedFor=%d, election timeout = %v, elapsed = %v, nextIndex = %+v, matchingIndex = %+v: %s\n",
		serverId, term, state, votedFor, electionTimeout, elapsed, nextIndex, matchingIndex, mesg)
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

func (rf *Raft) applier(applyCh chan raftapi.ApplyMsg) {
	for {
		rf.lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			entry := rf.log[rf.lastApplied]
			applyIndex := rf.lastApplied
			rf.unlock()

			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: applyIndex,
			}
			applyCh <- msg

			rf.lock()
		}
		rf.unlock()

		if rf.killed() {
			break
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func (rf *Raft) leaderInit() {
	DLog("Server %d: leaderInit begin\n", rf.me)

	lastLogIndex := len(rf.log) - 1
	npeers := len(rf.peers)

	// Allocate if not allocated yet
	if rf.nextIndex == nil || len(rf.nextIndex) != npeers {
		rf.nextIndex = make([]int, npeers)
	}
	if rf.matchingIndex == nil || len(rf.matchingIndex) != npeers {
		rf.matchingIndex = make([]int, npeers)
	}

	for i := 0; i < npeers; i++ {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchingIndex[i] = 0
	}

	// Leader's own match index is always the last entry
	rf.matchingIndex[rf.me] = lastLogIndex

	DLog("Server %d: leaderInit end\n", rf.me)
}

func (rf *Raft) lock() {
	rf.mu.Lock()
	// DTrace("LOCK %d\n", rf.me)
}

func (rf *Raft) unlock() {
	// DTrace("UNLOCK %d\n", rf.me)
	rf.mu.Unlock()
}

func (rf *Raft) readLock() {
	rf.mu.RLock()
	// DTrace("READ LOCK %d\n", rf.me)
}

func (rf *Raft) readUnlock() {
	// DTrace("READ UNLOCK %d\n", rf.me)
	rf.mu.RUnlock()
}
