package memory

import (
	"cyberflat/stq/backends"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Memory struct {
	queues     sync.Map
	inProcess  sync.Map
	readyTasks sync.Map

	taskIDCounter uint64
}

func New() (*Memory, error) {
	return &Memory{}, nil
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
		ID:      taskID,
		Payload: payload,
	})
	return taskID, nil
}

/*
	queue pool => task
	task => inprocess map
	timeout: delete(inprocess, task); task+error => ready map
*/
func (m *Memory) GetNotReady(queue string) (taskID string, payload []byte, err error) {
	q, ok := m.queues.Load(queue)
	if !ok {
		return "", nil, backends.ErrQueueNotFound
	}
	taskObject := q.(*sync.Pool).Get()
	task := taskObject.(*backends.Task)
	m.inProcess.Store(task.ID, task)
	go func() {
		time.Sleep(task.Timeout)
		m.inProcess.Delete(task.ID)
		task.Error = backends.ErrTaskExecutionTimeout
		m.readyTasks.Store(task.ID, task)
	}()
	return task.ID, task.Payload, nil
}

/*
	ready map => task
	delete(ready, task)
	return result
*/
func (m *Memory) GetReady(taskID string) (result []byte, err error) {
	taskObject, ok := m.readyTasks.Load(taskID)
	if !ok {
		return nil, backends.ErrTaskNotFoundOrNotReady
	}
	task := taskObject.(*backends.Task)
	if task.Error != nil {
		return nil, task.Error
	}
	result = task.Result
	m.readyTasks.Delete(taskID)
	return result, nil
}

/*
	inprocess map => task
	delete(inprocess, task)
	task => ready map
*/
func (m *Memory) TaskReady(taskID string, result []byte) error {
	taskObject, ok := m.inProcess.Load(taskID)
	if !ok {
		return backends.ErrTaskNotFoundOrNotReady
	}
	task := taskObject.(*backends.Task)
	task.Result = result
	m.inProcess.Delete(taskID)
	m.readyTasks.Store(taskID, task)
	return nil
}
