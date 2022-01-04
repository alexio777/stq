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

func (m *Memory) Put(queue string, payload []byte) (taskID string, err error) {
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

func (m *Memory) GetNotReady(queue string, timeoutInProcess time.Duration) (taskID string, payload []byte, err error) {
	q, ok := m.queues.Load(queue)
	if !ok {
		return "", nil, backends.ErrQueueNotFound
	}
	taskObject := q.(*sync.Pool).Get()
	task := taskObject.(*backends.Task)
	m.inProcess.Store(task.ID, task)
	go func() {
		time.Sleep(timeoutInProcess)
		m.inProcess.Delete(task.ID)
		task.Error = backends.ErrTaskExecutionTimeout
		m.readyTasks.Store(task.ID, task)
	}()
	return task.ID, task.Payload, nil
}

func (m *Memory) GetReady(taskID string) (result []byte, err error) {
	task, ok := m.readyTasks.Load(taskID)
	if !ok {
		return nil, backends.ErrTaskNotFoundOrNotReady
	}
	result = task.(*backends.Task).Result
	m.readyTasks.Delete(taskID)
	return result, nil
}

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
