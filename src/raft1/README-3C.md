# Lab 3C: Persistence

Please refer to README-3A and README-3B first.

Per the Raft paper, the following three states must be persisted on all servers:

  1. `currentTerm`: It prevents a restarted server from reverting to an older term and violating monotonicity of terms.

  2. `votedFor`: It prevents a server from revoting in the same term after a crash, which could allow multiple leaders to be elected in one term.

  3. `log[]`: The log must survive crashes to maintain the replicated log and avoid breaking the state machine consistency guarantees.

