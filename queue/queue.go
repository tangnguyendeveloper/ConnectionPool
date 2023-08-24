package queue

import (
	"fmt"
)

type Queue struct {
	elements []interface{}
	MaxSize  uint64
}

func (q *Queue) Enqueue(element interface{}) error {
	if len(q.elements) == int(q.MaxSize) {
		return fmt.Errorf("ERROR: Queue Overflow, max_size = %d", q.MaxSize)
	}
	q.elements = append(q.elements, element)
	return nil
}

func (q *Queue) Dequeue() interface{} {
	if q.IsEmpty() {
		return nil
	}

	element := q.elements[0]
	if q.Length() == 1 {
		q.elements = nil
		return element
	}

	q.elements = q.elements[1:]
	return element
}

func (q Queue) Peek(index uint64) interface{} {
	if q.IsEmpty() || index > (uint64(q.Length()-1)) {
		return nil
	}
	return q.elements[index]
}

func (q *Queue) Remove(index uint64) error {
	if q.IsEmpty() || index > (uint64(q.Length()-1)) {
		return fmt.Errorf("ERROR: out of size or Queue is empty")
	}

	q.elements = append(q.elements[:index], q.elements[index+1:]...)

	return nil
}

func (q Queue) Length() int {
	return len(q.elements)
}

func (q Queue) IsEmpty() bool {
	return len(q.elements) == 0
}

func (q Queue) String() string {
	return fmt.Sprintf("Queue %v", q.elements[:])
}
