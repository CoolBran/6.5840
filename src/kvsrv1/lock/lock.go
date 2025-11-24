package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockName string
	locker   string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockName = l
	lk.locker = kvtest.RandValue(8)
	return lk
}

// ck.Get && ck.Put have the retry
func (lk *Lock) Acquire() {
	// Your code here

	// too many errors in logic
	// value, version, ok := lk.ck.Get(lk.lockName)
	// for (ok != rpc.OK && ok != rpc.ErrNoKey) || (value != "" && value != lk.locker) {
	// 	value, version, ok = lk.ck.Get(lk.lockName)
	// 	time.Sleep(100 * time.Millisecond)
	// }
	// for ok == lk.ck.Put(lk.lockName, lk.locker, version) {
	// 	if ok == rpc.OK {
	// 		return
	// 	}
	// 	time.Sleep(100 * time.Millisecond)
	// }
	// }

	//Acquire need retry (Spinlock: 自旋锁)
	for {
		value, version, ok := lk.ck.Get(lk.lockName)
		if ok == rpc.ErrNoKey || (ok == rpc.OK && value == "") { //think of the case legal is more correct
			ok = lk.ck.Put(lk.lockName, lk.locker, version)

			if ok == rpc.OK {
				return
			}
		} else if ok == rpc.OK && value == lk.locker {
			return
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (lk *Lock) Release() {
	// Your code here

	//Release but not Acquire first[the case of reason wrong]
	// for {
	// 	value, version, ok := lk.ck.Get(lk.lockName)
	// 	if ok == rpc.OK && value == lk.locker {
	// 		err := lk.ck.Put(lk.lockName, "", version) //block
	// 		if err == rpc.OK {
	// 			return
	// 		}
	// 	}
	// 	time.Sleep(time.Millisecond * 100)
	// }

	value, version, ok := lk.ck.Get(lk.lockName)
	if ok == rpc.OK && value == lk.locker {
		lk.ck.Put(lk.lockName, "", version) //block
	}
}
