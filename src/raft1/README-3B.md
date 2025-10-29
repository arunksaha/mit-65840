# Lab 3B: Log

## Intro

Please see README-3A.md for the design of Lab 3A.

The goal of this lab (3B) is to implement log replication:
  - Leaders accept new log entries through the method `Start()`.
  - Leaders send them to followers via `AppendEntries()` RPC.
  - Followers append them to their logs if consistent.
  - Once a majority acknowledges, the entry is committed and applied.

No persistence of snapshots yet, just in-memory logs and commit.

## Overview

Following is the essential data structures for each Raft instance:
  - currentTerm: the current term (set to 0 on initialization and kill)
  - state: one among {Follower, Candidate, Leader} starting at Follower
  - votedFor: which server did this vote for, if voted, or null
  - lastHeard: time instant of last message from leader
  - heartbeatInterval: all servers have the same interval for
    sending heartbeats when they are leader
  - electionTimeout: all servers have a random timeout between
    300ms and 500ms; the exact value is chosen at init and
    whenever the server goes to the Follower state.

Following are added for 3B:

  - `log []logEntry`: log entries; each entry contains command for state machine, and
    when entry was received by leader. (first index is 1)

  - `commitIndex int`: index of highest log entry known to be committed
    (initialized to 0, increases monotonically)
    It is the highest log index known to be committed in the Raft cluster.
    A log entry is considered *committed* when:
      - It is stored on a majority of servers (including the Leader), and
      - It is in the Leader's current term (safety property)
    Once an entry is committed, it is guaranteed to stay in the log of any future leader.

  - `lastApplied int`: index of highest log entry applied to state machine
    (initialized to 0, increases monotonically)

Following state are maintained on leaders and reiniitalized after election:

  - `nextIndex []int`: for each server, index of the next log entry to send to that server
    (initialized to leader last log index + 1)

  - `matchingIndex []int`: for each server, index of the highest log entry
    known to be replicated on server
    (initialized to 0, increases monotonically)
    It is the highest log index a Follower has stored.

Two RPCs are used:
   - RequestVote
   - AppendEntries

## General RPC rule

In the Raft consensus algorithm, the rule that a receiver updates its term and 
reverts to Follower state upon receiving an RPC with a higher term is
a fundamental Term Safety measure.
This mechanism prevents old leaders or candidates from disrupting the cluster and 
ensures that a server with the most up-to-date information always becomes the leader.

## RequestVote RPC

If a Follower hasn't received any heartbeat for electionTimeout,
then it promotes itself to Candidate, increments its term and
sends RequestVote to all other peers.

The request has four parameters:
  - Term: Candidate's term
  - CandidateId: Candidate's server id
  - LastLogIndex: index of of Candidate's last log entry
  - LastLogTerm: term of Candidate's last log entry

The reply has two parameters:
  - CurrentTerm: current term of responder
  - VoteGranted: boolean, true iff responder voted this candidate

### Responder's logic

The RAFT paper says:

> Each server will vote for at most one candiate in a given term, on a first-come-first-serve basis.

The response processing logic consist of the following:
   
  - The default response is vote **not** granted, unless explicitly
    granted per the algorithm below.

  - If the responder is a older/lower term than the candidate, then
    the responder is transitioned to a follower with the candidate's term,
    and all voting history is erased.
    This ensures term monotonicity across all servers.

  - If the responder term is same as the candidate (possibly due to the above
    update), then vote is granted if:
      - vote is granted if the server has not yet voted in this term (votedFor == Null)
         or has already voted for this candidate.
      - candidate's log is at least as up-to-date as receiver's log
  ```
  lastLogIndex = len(rf.log) - 1
  lastLogTerm = rf.log[lastLogIndex].Term
  (args.lastLogTerm > lastLogTerm) ||
  ((args.LastLogTerm == lastLogTerm) && (args.LastLogIndex >= lastLogIndex))
  ```

  - If vote is granted, then the last heartbeat timestamp is updated,
    as any message from leader/candidate is considered an heartbeat.
    Without this, the election timer might trigger unnecessarily.

  - In all cases, the responder always send her `currentTerm` (which might have
    been updated during the current processing, see above) in the reply.

## AppendEntries RPC

This RPC is used to send heartbeat.

The request has the following parameters:
  - Term: leader's term
  - LeaderId: leader's server id
  - PrevLogIndex: Index of log entry immediately preceding new ones
  - PrevLogTerm: Term of PrevLogIndex entry
  - Entries[]: Log entries to store (empty for heartbeat, more than one for efficiency)
  - LeaderCommit: Leader's commitIndex

The reply has two parameters:
  - Term: follower's current term
  - Success: true only if follower's log contain an entry at `PrevLogIndex`
    whose term matches `PrevLogTerm`.

### Responder's logic

  - In all cases, the responder always send its `currentTerm` in the reply.
   
  - If the follower is at a higher term than the received message, then
    set `Success` to false. No transitions, no appends. Return immediately.

  - If the follower is at an older term, then the follower is updated
    to catch up to leader's term and any existing voting history is erased.

  - If `rf.log` does not contain an entry at `PrevLogIndex` or if that entry's
    term does not match `PrevLogTerm`, then set `Success` to false.

  - If an existing entry conflicts with a new one (same index but different terms),
    delete the existing entry and all that follow it.
    This ensures that the follower's log becomes a prefix of the leader's log.
    This truncation step happens only after the log consistency checks pass and
    before appending new entries.

  - Append any new entries not already in log.

  - Possibly update receiver's `commitIndex` as follows:
    ```
      if args.LeaderCommit > rf.commitIndex {
        rf.commitIndex = min(args.LeaderCommit, index of the last new entry)
      }
    ```
    This ensures that the follower does not mark entries as committed
    that the leader hasn't safely replicated on a majority.

  - The `lastHeard` timestamp is updated.

## Ticker

The ticker is started during initialization as a separate goroutine.
It runs as long as the Raft instance is alive (i.e., not killed).
It checks if the time since the last heartbeat has exceeded the election timeout.
If the timeout has expired and it is not a Leader, it triggers an election.
Candidates may also restart elections if they timeout before winning.
Otherwise, it sleeps for a random time duration between 50ms and 350ms.

## Initialization 

The Raft instance is initialized via `raftInit()`. It is called when the
instance is first created and every time the instance is "killed" (`Kill()`)

The election timeout value is re-chosen during each:
  - initialization (`raftInit()`) and
  - transition to follower (`transitionToFollower()`)

The ticker go routine is started (once) after the instance is created.

## Election

When an election is started (due to election timeout), the Follower changes her
state to Candidate and increases her term number by one. She also votes for herself.

The Candidate sends RequestVote RPC to all the peers. If the RPC succeeds and this
instance is still the Candidate, the responses are processed.

If the reply carries a higher term from the responder, then this instance transitions
to Follower and (logically) ends the current election.

As part of processing, if the responder has granted vote,
then Candidate increments the vote count.
If the vote count reaches the quorum, then this Candidate instance
 transitions to Leader and start sending heartbeats.

## Heartbeats

Once a Candidate wins the election, it starts a goroutine to send heartbeats.
The goroutine continues (infinitely) as long as the instance continues to be
*alive* in the Leader state.

It sends heartbeat to all the peers using the AppendEntries RPC.
If the RPC response shows that the responder is at a higher term,
this instance transitions from Leader to Follower.
The heartbeat goroutine exits subsequently.

## Misc

### Locking
As a concurrent server, any access to the Raft object is protected by lock.
A RWMutex protects all access to the Raft object.
Read locks are used for inspection or logging; write locks for updates.
This improves concurrency since many operations (like logging) are read-only.

### Logging
Debugging the server requires visibility via logging.
Instead of repeating ad-hoc log messages throughout, a method (`DLogState()`)
is used to safely read the object state safely and log those contents.

### Election timeout
The `electionTimeout` is reset at different situations:
  - server initialization
  - server killed
  - server transitioned to Follower

Randomization of election timeouts ensures that servers don’t repeatedly
start elections at the same time, which reduces the likelihood of split votes.

## Testing
