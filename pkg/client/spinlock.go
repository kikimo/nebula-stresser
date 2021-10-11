package client

import "sync/atomic"

const (
	SpinUnlocked = 0
	SpinLocked   = 1
)

type Spinlock struct {
	val int32
}

func (l *Spinlock) Lock() {
	atomic.CompareAndSwapInt32(&l.val, SpinUnlocked, SpinLocked)
}

func (l *Spinlock) Unlock() {
	atomic.StoreInt32(&l.val, SpinUnlocked)
}

func NewSpinlock() *Spinlock {
	return &Spinlock{
		val: SpinUnlocked,
	}
}
