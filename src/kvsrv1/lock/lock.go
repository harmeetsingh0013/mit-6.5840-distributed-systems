package lock

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strings"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

const (
	LockHeld = "HELD"
	LockFree = "FREE"
)

type Lock struct {
	ck        kvtest.IKVClerk
	localname string
	id        int64
}

func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, localname: lockname, id: generateRandomID()}
	return lk
}

func (lk *Lock) Acquire() {
	HELD := LockHeld + "-" + fmt.Sprintf("%d", lk.id)
	for {
		value, version, err := lk.ck.Get(lk.localname)
		if err == rpc.ErrNoKey {
			if !putKV(lk, 0, HELD) {
				continue
			}
			return
		}
		switch strings.Split(value, "-")[0] {
		case LockFree:
			if !putKV(lk, version, HELD) {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			return
		case LockHeld:
			time.Sleep(10 * time.Millisecond)
			continue
		}
	}

}

func (lk *Lock) Release() {
	HELD := LockHeld + "-" + fmt.Sprintf("%d", lk.id)
	value, version, err := lk.ck.Get(lk.localname)
	if err == rpc.ErrNoKey {
		return
	}

	switch value {
	case HELD:
		putKV(lk, version, LockFree)
		return
	default:
		return
	}

}

func generateRandomID() int64 {
	// Generates a random number in range [0, 1_000_000_000_000)
	n, _ := rand.Int(rand.Reader, big.NewInt(1_000_000_000_000))
	return n.Int64()
}

func putKV(lk *Lock, version rpc.Tversion, uniqueStatus string) bool {
	putError := lk.ck.Put(lk.localname, uniqueStatus, version)
	if putError == rpc.ErrMaybe {
		value, _, _ := lk.ck.Get(lk.localname)
		if value == uniqueStatus {
			return true
		}
		return false
	}
	if putError == rpc.OK {
		return true
	}
	return false
}
