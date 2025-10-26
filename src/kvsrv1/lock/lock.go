package lock

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	key   string
	value string
}

const RandSeed = 12345678

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	randval := kvtest.RandValue(RandSeed)
	lk.key = l
	lk.value = randval
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	DPrintf("Acquiring lock for key %s with value %s\n", lk.key, lk.value)
	// Implementation to acquire the lock
	var version rpc.Tversion
	var gresult, presult rpc.Err
	for {
		presult = lk.ck.Put(lk.key, lk.value, 0)
		if presult == rpc.OK {
			break
		}
		_, version, gresult = lk.ck.Get(lk.key)
		if gresult == rpc.OK {
			if Even(uint64(version)) {
				presult = lk.ck.Put(lk.key, lk.value, version)
				done := presult == rpc.OK || presult == rpc.ErrMaybe || presult == rpc.ErrNoKey
				if done {
					break
				}
			} else {
				log.Printf("Acquire version=%d unexpected, release first?\n", version)
			}
		}
		BusyWait()
	}
}

func (lk *Lock) Release() {
	// Your code here
	DPrintf("Releasing lock for key %s with value %s\n", lk.key, lk.value)
	// Implementation to release the lock
	var val string
	var version rpc.Tversion
	var gresult, presult rpc.Err
	for {
		val, version, gresult = lk.ck.Get(lk.key)
		if gresult == rpc.OK {
			if !Even(uint64(version)) && (val == lk.value) {
				presult = lk.ck.Put(lk.key, lk.value, version)
				done := presult == rpc.OK || presult == rpc.ErrMaybe || presult == rpc.ErrNoKey
				if done {
					break
				}
			}
		}
		BusyWait()
	}
}

func Even(n uint64) bool {
	return n%2 == 0
}

func BusyWait() {
	busywait := 1 * time.Millisecond
	time.Sleep(busywait)
}
