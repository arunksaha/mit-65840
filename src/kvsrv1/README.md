# Key-Value server

Initial run

```
311e5c5 2025-10-25 Arun Saha (HEAD -> lab2-kvsrv) kvsrv1: part 1: key-value server with reliable network

Sat Oct 25 04:43:17 PM PDT 2025
=== RUN   TestReliablePut
One client and reliable Put (reliable network)...
labgob warning: Decoding into a non-default variable/field Err may not work
  ... Passed --  time  0.0s #peers 1 #RPCs    14 #Ops    0
--- PASS: TestReliablePut (0.00s)
=== RUN   TestPutConcurrentReliable
Test: many clients racing to put values to the same key (reliable network)...
  ... Passed --  time  1.5s #peers 1 #RPCs 25073 #Ops 25073
--- PASS: TestPutConcurrentReliable (1.46s)
=== RUN   TestMemPutManyClientsReliable
Test: memory use many put clients (reliable network)...
  ... Passed --  time 10.7s #peers 1 #RPCs 100000 #Ops    0
--- PASS: TestMemPutManyClientsReliable (89.58s)
PASS
ok  	6.5840/kvsrv1	91.061s
Sat Oct 25 04:44:49 PM PDT 2025
```

Final run
```
3495564 2025-10-26 Arun Saha (HEAD -> lab2-final) change printf to conditional debug

[src/kvsrv1] $ go test -v
=== RUN   TestReliablePut
One client and reliable Put (reliable network)...
labgob warning: Decoding into a non-default variable/field Err may not work
  ... Passed --  time  0.0s #peers 1 #RPCs    14 #Ops    0
--- PASS: TestReliablePut (0.00s)
=== RUN   TestPutConcurrentReliable
Test: many clients racing to put values to the same key (reliable network)...
  ... Passed --  time  1.2s #peers 1 #RPCs 25255 #Ops 25255
--- PASS: TestPutConcurrentReliable (1.22s)
=== RUN   TestMemPutManyClientsReliable
Test: memory use many put clients (reliable network)...
  ... Passed --  time 10.6s #peers 1 #RPCs 100000 #Ops    0
--- PASS: TestMemPutManyClientsReliable (89.33s)
=== RUN   TestUnreliableNet
One client (unreliable network)...
  ... Passed --  time  3.7s #peers 1 #RPCs   253 #Ops  207
--- PASS: TestUnreliableNet (3.69s)
PASS
ok  	6.5840/kvsrv1	94.280s

[src/kvsrv1/lock]$ go test -v
go test -v
=== RUN   TestOneClientReliable
Test: 1 lock clients (reliable network)...
  ... Passed --  time  2.1s #peers 1 #RPCs   175 #Ops    0
--- PASS: TestOneClientReliable (2.06s)
=== RUN   TestManyClientsReliable
Test: 10 lock clients (reliable network)...
  ... Passed --  time  9.1s #peers 1 #RPCs    10 #Ops    0
--- PASS: TestManyClientsReliable (9.06s)
=== RUN   TestOneClientUnreliable
Test: 1 lock clients (unreliable network)...
  ... Passed --  time  2.0s #peers 1 #RPCs    71 #Ops    0
--- PASS: TestOneClientUnreliable (2.01s)
=== RUN   TestManyClientsUnreliable
Test: 10 lock clients (unreliable network)...
  ... Passed --  time  9.0s #peers 1 #RPCs    14 #Ops    0
--- PASS: TestManyClientsUnreliable (9.03s)
PASS
ok  	6.5840/kvsrv1/lock	22.188s

```
