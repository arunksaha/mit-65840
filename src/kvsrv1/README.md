# Key-Value server

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

