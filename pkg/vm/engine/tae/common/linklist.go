// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import "sync"

type ISLLNode interface {
	IRef
}

type ISSLLNode interface {
	Insert(ISSLLNode)
	GetNext() ISSLLNode
	SetNext(ISSLLNode)
	ReleaseNextNode() ISSLLNode
	ReleaseFollowing() ISSLLNode
}

type SSLLNode struct {
	next ISSLLNode
}

func NewSSLLNode() *SSLLNode {
	return new(SSLLNode)
}

func (n *SSLLNode) Insert(nn ISSLLNode) {
	next := n.next
	n.next = nn
	nn.SetNext(next)
}

func (n *SSLLNode) SetNext(next ISSLLNode) {
	n.next = next
}

func (n *SSLLNode) GetNext() ISSLLNode {
	return n.next
}

func (n *SSLLNode) ReleaseNextNode() ISSLLNode {
	if n.next != nil {
		next := n.next.GetNext()
		r := n.next
		n.next = next
		return r
	}
	return nil
}

func (n *SSLLNode) ReleaseFollowing() ISSLLNode {
	if n.next != nil {
		next := n.next
		n.next = nil
		return next
	}
	return nil
}

// SLLNode represent a single node in linked list.
// It is thread-safe.
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
	l.SetNextNodeNoLock(next)
}

func (l *SLLNode) SetNextNodeNoLock(next ISLLNode) {
	if l.Next != nil {
		l.Next.Unref()
	}
	l.Next = next
}

func (l *SLLNode) Insert(n ISLLNode) {
	l.Lock()
	defer l.Unlock()
	next := l.Next
	l.Next = n
	n.(*SLLNode).Next = next
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
