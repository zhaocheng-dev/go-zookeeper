package zk

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrDeadlock is returned by Lock when trying to lock twice without unlocking first
	ErrDeadlock = errors.New("zk: trying to acquire a lock twice")
	// ErrNotLocked is returned by Unlock when trying to release a lock that has not first be acquired.
	ErrNotLocked = errors.New("zk: not locked")
	// ErrTimeOut is returned by
	ErrTimeOut = errors.New("zk:lock fail cause time out")
)

// Lock is a mutual exclusion lock.
type Lock struct {
	c             *Conn
	path          string
	acl           []ACL
	lockPath      string
	seq           int
	lowestSeqPath string
}

// NewLock creates a new lock instance using the provided connection, path, and acl.
// The path must be a node that is only used by this lock. A lock instances starts
// unlocked until Lock() is called.
func NewLock(c *Conn, path string, acl []ACL) *Lock {
	return &Lock{
		c:    c,
		path: path,
		acl:  acl,
	}
}

func parseSeq(path string) (int, error) {
	parts := strings.Split(path, "-")
	return strconv.Atoi(parts[len(parts)-1])
}

func (l *Lock) TryLock(ctx context.Context, retry int) (bool, error) {
	if l.lockPath != "" {
		return false, ErrDeadlock
	}

	prefix := fmt.Sprintf("%s/lock-", l.path)

	path := ""
	var err error
	for i := 0; i < retry; i++ {
		path, err = l.c.CreateProtectedEphemeralSequential(prefix, []byte{}, l.acl)
		if err == ErrNoNode {
			// Create parent node.
			parts := strings.Split(l.path, "/")
			pth := ""
			for _, p := range parts[1:] {
				var exists bool
				pth += "/" + p
				exists, _, err = l.c.Exists(pth)
				if err != nil {
					return false, err
				}
				if exists == true {
					continue
				}
				_, err = l.c.Create(pth, []byte{}, 0, l.acl)
				if err != nil && err != ErrNodeExists {
					return false, err
				}
			}
		} else if err == nil {
			break
		} else {
			return false, err
		}
	}
	if err != nil {
		return false, err
	}

	seq, err := parseSeq(path)
	if err != nil {
		return false, err
	}

	children, _, err := l.c.Children(l.path)
	if err != nil {
		return false, err
	}

	lowestSeq := seq
	for _, p := range children {
		s, err := parseSeq(p)
		if err != nil {
			return false, err
		}
		if s < lowestSeq {
			lowestSeq = s
			l.lowestSeqPath = p
		}
	}

	l.seq = seq
	l.lockPath = path
	if seq != lowestSeq {
		// Acquired the lock
		return false, nil
	}
	return true, nil
}

// Lock attempts to acquire the lock. It will wait to return until the lock
// is acquired or an error occurs. If this instance already has the lock
// then ErrDeadlock is returned.
func (l *Lock) Lock(ctx context.Context) (bool, error) {
	return l.innerLock(ctx, 3)
}

func (l *Lock) LockWithTime(ctx context.Context, duration time.Duration, retry int) (bool, error) {
	ctx, _ = context.WithTimeout(ctx, duration)
	return l.innerLock(ctx, retry)
}

func (l *Lock) innerLock(ctx context.Context, retry int) (bool, error) {
	if l.lockPath != "" {
		return false, ErrDeadlock
	}

	hasAcquire, err := l.TryLock(ctx, retry)

	if err != nil {
		return false, err
	}
	if hasAcquire {
		return hasAcquire, err
	}

	seq, err := parseSeq(l.lockPath)
	if err != nil {
		return false, err
	}
	for {
		children, _, err := l.c.Children(l.path)
		if err != nil {
			return false, err
		}

		lowestSeq := seq
		prevSeq := -1
		prevSeqPath := ""
		for _, p := range children {
			s, err := parseSeq(p)
			if err != nil {
				return false, err
			}
			if s < lowestSeq {
				lowestSeq = s
			}
			if s < seq && s > prevSeq {
				prevSeq = s
				prevSeqPath = p
			}
		}

		if seq == lowestSeq {
			// Acquired the lock
			break
		}

		// Wait on the node next in line for the lock
		_, _, ch, err := l.c.GetW(l.path + "/" + prevSeqPath)
		if err != nil && err != ErrNoNode {
			return false, err
		} else if err != nil && err == ErrNoNode {
			// try again
			continue
		}

		select {
		case ev := <-ch:
			if ev.Err != nil {
				return false, ev.Err
			}
		case <-ctx.Done():
			return false, ErrTimeOut
		}
	}
	return true, nil
}

// Unlock releases an acquired lock. If the lock is not currently acquired by
// this Lock instance than ErrNotLocked is returned.
func (l *Lock) Unlock() error {
	if l.lockPath == "" {
		return ErrNotLocked
	}
	if err := l.c.Delete(l.lockPath, -1); err != nil {
		return err
	}
	l.lockPath = ""
	l.seq = 0
	return nil
}
