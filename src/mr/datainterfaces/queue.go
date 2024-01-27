package queue

import "github.com/google/uuid"

type Queue []uuid.UUID

func (self *Queue) Push(x uuid.UUID) {
	*self = append(*self, x)
}

func (self *Queue) Pop() (uuid.UUID, bool) {
	h := *self
	var el uuid.UUID

	l := len(h)
	if l > 0 {
		el, *self = h[0], h[1:l]
		// Or use this instead for a Stack
		// el, *self = h[l-1], h[0:l-1]
		return el, false
	}

	return el, true
}

func (self *Queue) IsEmpty() bool {
	h := *self
	return len(h) == 0
}

func NewQueue() *Queue {
	return &Queue{}
}

// Example usage
//   q := NewQueue()
//   q.Push(1)
//   q.Push(2)
//   q.Push(3)
//   q.Push("L")
