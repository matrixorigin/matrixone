// Copyright 2023 Matrix Origin
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

package interval

// Interval [low, high)
type Interval struct {
	low, high int64
}

type Node struct {
	interval    Interval
	maxEndpoint int64
	left, right *Node
	isRed       bool
}

func NewNode(interval Interval) *Node {
	return &Node{
		interval:    interval,
		maxEndpoint: interval.high,
		left:        nil,
		right:       nil,
		isRed:       true,
	}
}

type IntTree struct {
	root *Node
}

func NewIntervalTree() *IntTree {
	return &IntTree{
		root: nil,
	}
}

func (t *IntTree) Insert(interval Interval) {
	t.root = t.insertNode(t.root, interval)
	t.root.isRed = false
}

func (t *IntTree) insertNode(node *Node, interval Interval) *Node {
	if node == nil {
		return NewNode(interval)
	}

	if interval.low < node.interval.low {
		node.left = t.insertNode(node.left, interval)
	} else {
		node.right = t.insertNode(node.right, interval)
	}

	node = t.fixup(node)

	return node
}

func (t *IntTree) Contains(interval Interval) bool {
	return t.containsNode(t.root, interval)
}

func (t *IntTree) containsNode(node *Node, interval Interval) bool {
	if node == nil {
		return false
	}

	if interval.low <= node.interval.low && interval.high >= node.interval.high {
		return true
	}

	if node.left != nil && interval.low <= node.left.maxEndpoint {
		return t.containsNode(node.left, interval)
	}

	return t.containsNode(node.right, interval)
}

func (t *IntTree) Remove(interval Interval) {
	if t.root == nil {
		return
	}

	t.root = t.removeNode(t.root, interval)
	if t.root != nil {
		t.root.isRed = false
	}
}

func (t *IntTree) removeNode(node *Node, interval Interval) *Node {
	if interval.low < node.interval.low {
		if node.left == nil {
			return node
		}
		if !t.isRed(node.left) && !t.isRed(node.left.left) {
			node = t.moveRedLeft(node)
		}
		node.left = t.removeNode(node.left, interval)
	} else {
		if t.isRed(node.left) {
			node = t.rotateRight(node)
		}
		if interval.low == node.interval.low && node.right == nil {
			return nil
		}
		if node.right != nil && !t.isRed(node.right) && !t.isRed(node.right.left) {
			node = t.moveRedRight(node)
		}
		if interval.low == node.interval.low {
			minNode := t.findMinNode(node.right)
			node.interval = minNode.interval
			node.right = t.deleteMinNode(node.right)
		} else {
			node.right = t.removeNode(node.right, interval)
		}
	}

	return t.fixup(node)
}

func (t *IntTree) fixup(node *Node) *Node {
	if t.isRed(node.right) && !t.isRed(node.left) {
		node = t.rotateLeft(node)
	}

	if t.isRed(node.left) && t.isRed(node.left.left) {
		node = t.rotateRight(node)
	}

	if t.isRed(node.left) && t.isRed(node.right) {
		t.flipColors(node)
	}

	node.maxEndpoint = t.max(t.maxEndpoint(node.left), t.maxEndpoint(node.right), node.interval.high)

	return node
}

func (t *IntTree) rotateLeft(node *Node) *Node {
	newRoot := node.right
	node.right = newRoot.left
	newRoot.left = node
	newRoot.isRed = node.isRed
	node.isRed = true

	newRoot.maxEndpoint = node.maxEndpoint
	node.maxEndpoint = t.max(t.maxEndpoint(node.left), t.maxEndpoint(node.right), node.interval.high)

	return newRoot
}

func (t *IntTree) rotateRight(node *Node) *Node {
	newRoot := node.left
	node.left = newRoot.right
	newRoot.right = node
	newRoot.isRed = node.isRed
	node.isRed = true

	newRoot.maxEndpoint = node.maxEndpoint
	node.maxEndpoint = t.max(t.maxEndpoint(node.left), t.maxEndpoint(node.right), node.interval.high)

	return newRoot
}

func (t *IntTree) flipColors(node *Node) {
	node.isRed = !node.isRed
	node.left.isRed = !node.left.isRed
	node.right.isRed = !node.right.isRed
}

func (t *IntTree) moveRedLeft(node *Node) *Node {
	t.flipColors(node)
	if node.right != nil && t.isRed(node.right.left) {
		node.right = t.rotateRight(node.right)
		node = t.rotateLeft(node)
		t.flipColors(node)
	}
	return node
}

func (t *IntTree) moveRedRight(node *Node) *Node {
	t.flipColors(node)
	if node.left != nil && t.isRed(node.left.left) {
		node = t.rotateRight(node)
		t.flipColors(node)
	}
	return node
}

func (t *IntTree) deleteMinNode(node *Node) *Node {
	if node.left == nil {
		return nil
	}

	if !t.isRed(node.left) && !t.isRed(node.left.left) {
		node = t.moveRedLeft(node)
	}

	node.left = t.deleteMinNode(node.left)
	return t.fixup(node)
}

func (t *IntTree) findMinNode(node *Node) *Node {
	if node == nil {
		return nil
	}
	if node.left == nil {
		return node
	}
	return t.findMinNode(node.left)
}

func (t *IntTree) maxEndpoint(node *Node) int64 {
	if node == nil {
		return 0
	}
	return node.maxEndpoint
}

func (t *IntTree) max(a, b, c int64) int64 {
	if a > b && a > c {
		return a
	} else if b > a && b > c {
		return b
	}
	return c
}

func (t *IntTree) isRed(node *Node) bool {
	if node == nil {
		return false
	}
	return node.isRed
}

func (t *IntTree) Size() int {
	return 0
}
