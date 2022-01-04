package backends

import "time"

type Task struct {
	ID      string
	AddedAt time.Time
	Payload []byte
}

type Backend interface {
	// Initialize the backend.
	New() (Backend, error)
	// Close the backend.
	Close() error
	// Get the backend name.
	Name() string
	// Put task to queue.
	Put(queue string, task *Task, timeout time.Duration) error
	// Get incomplete task from queue.
	Get(queue string) (*Task, error)
	// Get complete task from queue.
	GetComplete(queue string) (*Task, error)
}
