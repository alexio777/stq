package memory

import (
	"bytes"
	"cyberflat/stq/backends"
	"sync"
	"testing"
	"time"
)

func Test_MemoryBackend(t *testing.T) {
	t.Run("Put", func(t *testing.T) {
		t.Run("Put task to queue", func(t *testing.T) {
			backend, err := New()
			if err != nil {
				t.Fatal(err)
			}
			taskID, err := backend.Put("queue", []byte("payload"), time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if taskID == "" {
				t.Fatal("taskID is empty")
			}
			queue, ok := backend.queues.Load("queue")
			if !ok {
				t.Fatal("queue not found")
			}
			taskObject := queue.(*sync.Pool).Get()
			if taskObject == nil {
				t.Fatal("taskObject is nil")
			}
			task := taskObject.(*backends.Task)
			if task.ID != taskID {
				t.Fatalf("taskID is not equal: %s != %s", task.ID, taskID)
			}
		})
	})
	t.Run("Get", func(t *testing.T) {
		backend, err := New()
		if err != nil {
			t.Fatal(err)
		}
		t.Run("Check execution timeout", func(t *testing.T) {
			taskID, err := backend.Put("queue", []byte("timeout"), time.Nanosecond)
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = backend.GetNotReady("queue")
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(time.Millisecond)
			task, err := backend.GetReady(taskID)
			if task != nil {
				t.Fatal("task is not nil")
			}
			if err != backends.ErrTaskExecutionTimeout {
				t.Fatalf("task execution timeout is not detected: %s", err)
			}
			q, ok := backend.queues.Load("queue")
			if !ok {
				t.Fatal("queue not found")
			}
			taskObject := q.(*sync.Pool).Get()
			if taskObject != nil {
				task := taskObject.(*backends.Task)
				if task != nil {
					t.Fatal("task is not nil")
				}
			}
			_, ok = backend.work.Load(taskID)
			if ok {
				t.Fatal("task is not deleted from inprocess")
			}
		})
		taskID, err := backend.Put("queue", []byte("payload"), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		notready_taskID, payload, err := backend.GetNotReady("queue")
		if err != nil {
			t.Fatal(err)
		}
		if taskID != notready_taskID {
			t.Fatalf("taskID is not equal: %s != %s", taskID, notready_taskID)
		}
		if string(payload) != "payload" {
			t.Fatalf("payload is not equal: %s != %s", string(payload), "payload")
		}
		q, ok := backend.queues.Load("queue")
		if !ok {
			t.Fatal("queue not found")
		}
		taskObject := q.(*sync.Pool).Get()
		if taskObject != nil {
			task := taskObject.(*backends.Task)
			if task != nil {
				t.Fatal("task is not nil")
			}
		}
		if err := backend.TaskReady(taskID, []byte("done")); err != nil {
			t.Fatal(err)
		}
		q, ok = backend.queues.Load("queue")
		if !ok {
			t.Fatal("queue not found")
		}
		taskObject = q.(*sync.Pool).Get()
		if taskObject != nil {
			task := taskObject.(*backends.Task)
			if task != nil {
				t.Fatal("task is not nil")
			}
		}
		_, ok = backend.work.Load(taskID)
		if ok {
			t.Fatal("task exist in in process queue")
		}
		task, ok := backend.ready.Load(taskID)
		if !ok {
			t.Fatal("ready tasks is empty")
		}
		if task.(*backends.Task).Result == nil {
			t.Fatal("task result is empty")
		}
		if task.(*backends.Task).Error != nil {
			t.Fatal("task error is not nil")
		}
		if !bytes.Equal(task.(*backends.Task).Result, []byte("done")) {
			t.Fatalf("task result is not equal: %s != %s", string(task.(*backends.Task).Result), "done")
		}
		result, err := backend.GetReady(taskID)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(result, []byte("done")) {
			t.Fatalf("result is not equal: %s != %s", string(result), "done")
		}
		task, ok = backend.ready.Load(taskID)
		if ok {
			t.Fatalf("task is not empty: %s", task.(*backends.Task).ID)
		}
	})
}
