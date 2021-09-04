package common

import "sync"

// SinglyLinkedList
type ISLLNode interface {
	IRef
}

type SLLNode struct {
	RefHelper
	*sync.RWMutex
	Next ISLLNode
}

func NewSLLNode(mu *sync.RWMutex) *SLLNode {
	mtx := mu
	if mtx == nil {
		mtx = &sync.RWMutex{}
	}
	return &SLLNode{
		RWMutex: mtx,
	}
}

func (l *SLLNode) SetNextNode(next ISLLNode) {
	l.Lock()
	defer l.Unlock()
	if l.Next != nil {
		l.Next.Unref()
	}
	l.Next = next
}

func (l *SLLNode) GetNextNode() ISLLNode {
	var r ISLLNode
	l.RLock()
	if l.Next != nil {
		l.Next.Ref()
		r = l.Next
	}
	l.RUnlock()
	return r
}

func (l *SLLNode) ReleaseNextNode() {
	l.Lock()
	defer l.Unlock()
	if l.Next != nil {
		l.Next.Unref()
		l.Next = nil
	}
}
