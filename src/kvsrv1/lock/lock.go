package lock

import (
	"fmt"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	ck        kvtest.IKVClerk
	localname string
}

func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, localname: lockname}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.localname)
		if err == rpc.ErrNoKey {
			putError := lk.ck.Put(lk.localname, "HELD", 0)
			if putError == rpc.OK {
				return
			}
			continue
		}
		switch value {
		case "FREE":
			putError := lk.ck.Put(lk.localname, "HELD", version)
			if putError == rpc.OK {
				return
			}
			time.Sleep(10 * time.Millisecond)
			continue
		case "HELD":
			time.Sleep(10 * time.Millisecond)
			continue
		}
	}

}

func (lk *Lock) Release() {
	for {
		value, version, err := lk.ck.Get(lk.localname)
		if err == rpc.ErrNoKey {
			fmt.Println("Release called on a lock that doesn't exist")
			return
		}

		switch value {
		case "HELD":
			putError := lk.ck.Put(lk.localname, "FREE", version)
			if putError != rpc.OK {
				fmt.Println("Release called on a lock that doesn't exist")
				continue
			}
			return
		default:
			fmt.Println("Release called on a lock that doesn't exist")
		}
	}

}
