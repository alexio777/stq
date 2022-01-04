package memory

import (
	"cyberflat/stq/backends"
	"sync"
	"time"
)

type Memory struct {
	queuesIncomplete sync.Map
	queuesComplete   sync.Map
}

func New() (*Memory, error) {
	return &Memory{}, nil
}

func Close() error {
	return nil
}

func (m *Memory) Name() string {
	return "memory"
}

func (m *Memory) Put(queue string, task *backends.Task, timeout time.Duration) error {
	q, ok := m.queuesIncomplete.Load(queue)
	if !ok {
		q = &sync.Pool{}
		m.queuesIncomplete.Store(queue, q)
	}
	q.(*sync.Pool).Put(task)
	return nil
}

func get(m *sync.Map, queue string) (*backends.Task, error) {
	q, ok := m.Load(queue)
	if !ok {
		return nil, nil
	}
	task := q.(*sync.Pool).Get()
	if task == nil {
		return nil, nil
	}
	return task.(*backends.Task), nil
}

func (m *Memory) Get(queue string) (*backends.Task, error) {
	return get(&m.queuesComplete, queue)
}

func (m *Memory) GetComplete(queue string) (*backends.Task, error) {
	return get(&m.queuesComplete, queue)
}
