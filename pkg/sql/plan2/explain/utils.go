package explain

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type Frame struct {
	node                 *plan.Node
	next_child           int
	is_description_print bool
}

//  Stack is the stack used to store plan.Node
type Stack struct {
	nums []*Frame
}

// NewStack returan *explain.Stack
func NewStack() *Stack {
	return &Stack{nums: []*Frame{}}
}

// Push Put n on the stack
func (s *Stack) Push(n *Frame) {
	s.nums = append(s.nums, n)
}

// Pop  Take the last value put on the stack from S
func (s *Stack) Pop() *Frame {
	res := s.nums[len(s.nums)-1]
	s.nums = s.nums[:len(s.nums)-1]
	return res
}

// Top Get last value of the stack
func (s *Stack) Top() *Frame {
	return s.nums[len(s.nums)-1]
}

// Size Returns the length of S
func (s *Stack) Size() int {
	return len(s.nums)
}

// Empty Returns whether s is empty
func (s *Stack) Empty() bool {
	return s.Size() == 0
}
