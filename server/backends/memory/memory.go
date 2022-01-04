package memory

import (
	"cyberflat/stq/server/backends"
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Memory struct {
	queues sync.Map
	work   sync.Map
	ready  sync.Map

	stats      map[string]backends.Stats
	statsMutex sync.Mutex

	taskIDCounter uint64
}

func New() (*Memory, error) {
	return &Memory{
		stats: make(map[string]backends.Stats),
	}, nil
}

func (m *Memory) Close() error {
	return nil
}

func (m *Memory) Name() string {
	return "memory"
}

/*
	task => queue pool
*/
func (m *Memory) Put(queue string, payload []byte, executionTimeout time.Duration) (taskID string, err error) {
	id := atomic.AddUint64(&m.taskIDCounter, 1)
	taskID = strconv.FormatUint(id, 10)
	q, ok := m.queues.Load(queue)
	if !ok {
		q = &sync.Pool{}
		m.queues.Store(queue, q)
	}
	q.(*sync.Pool).Put(&backends.Task{
		Queue:   queue,
		ID:      taskID,
		Payload: payload,
		Timeout: executionTimeout,
	})
	m.updateStats(queue, func(stats *backends.Stats) {
		stats.WaitLength++
	})
	return taskID, nil
}

/*
	queue pool => task
	task => executed map
	timeout: delete(executed, task); task+error => ready map
*/
func (m *Memory) GetNotReady(queue string) (taskID string, payload []byte, err error) {
	q, ok := m.queues.Load(queue)
	if !ok {
		return "", nil, backends.ErrQueueNotFound
	}
	taskObject := q.(*sync.Pool).Get()
	if taskObject == nil {
		return "", nil, backends.ErrQueueNotFound
	}
	task := taskObject.(*backends.Task)
	m.work.Store(task.ID, task)
	m.updateStats(queue, func(stats *backends.Stats) {
		stats.WaitLength--
		stats.WorkLength++
	})
	go func() {
		time.Sleep(task.Timeout)
		m.work.Delete(task.ID)
		task.Error = backends.ErrTaskExecutionTimeout
		m.ready.Store(task.ID, task)
	}()
	return task.ID, task.Payload, nil
}

/*
	ready map => task
	delete(ready, task)
	return result
*/
func (m *Memory) GetReady(taskID string) (result []byte, err error) {
	taskObject, ok := m.ready.Load(taskID)
	if !ok {
		return nil, backends.ErrTaskNotFoundOrNotReady
	}
	task := taskObject.(*backends.Task)
	if task.Error != nil {
		return nil, task.Error
	}
	result = task.Result
	m.ready.Delete(taskID)
	m.updateStats(task.Queue, func(stats *backends.Stats) {
		stats.ReadyLength--
	})
	return result, nil
}

/*
	executed map => task
	delete(executed, task)
	task => ready map
*/
func (m *Memory) TaskReady(taskID string, result []byte) error {
	taskObject, ok := m.work.Load(taskID)
	if !ok {
		return backends.ErrTaskNotFoundOrNotReady
	}
	task := taskObject.(*backends.Task)
	task.Result = result
	m.work.Delete(taskID)
	m.ready.Store(taskID, task)
	m.updateStats(task.Queue, func(stats *backends.Stats) {
		stats.WorkLength--
		stats.ReadyLength++
	})
	return nil
}

func (m *Memory) Stats() ([]byte, error) {
	m.statsMutex.Lock()
	data, err := json.MarshalIndent(m.stats, "", "  ")
	m.statsMutex.Unlock()
	return data, err
}

func (m *Memory) updateStats(queue string, cb func(stats *backends.Stats)) {
	m.statsMutex.Lock()
	stats, ok := m.stats[queue]
	if !ok {
		stats = backends.Stats{}
	}
	cb(&stats)
	m.stats[queue] = stats
	m.statsMutex.Unlock()
}
