package backends

import (
	"errors"
	"time"
)

var (
	ErrQueueNotFound          = errors.New("queue not found")
	ErrTaskNotFoundOrNotReady = errors.New("task not found or not ready")
	ErrTaskExecutionTimeout   = errors.New("task execution timeout")
)

type Stats struct {
	WaitLength  uint64
	WorkLength  uint64
	ReadyLength uint64
}

type Task struct {
	Queue   string
	ID      string
	Payload []byte
	Error   error
	Result  []byte
	Timeout time.Duration
}

type Backend interface {
	// Close the backend.
	Close() error
	// Get the backend name.
	Name() string
	// Put task to queue and return task id.
	Put(queue string, payload []byte, executionTimeout time.Duration) (taskID string, err error)
	// Get not ready task from queue and start processing timeout.
	GetNotReady(queue string) (taskID string, payload []byte, err error)
	// Get ready task by task id or task error.
	GetReady(taskid string) (result []byte, err error)
	// Task is ready.
	TaskReady(taskid string, result []byte) error
	// Queues stats
	Stats() ([]byte, error)
}
