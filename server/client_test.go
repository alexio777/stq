package main

import (
	"bytes"
	"context"
	"cyberflat/stq/client"
	"cyberflat/stq/server/backends/memory"
	"log"
	"net"
	"testing"
	"time"
)

func Test_Client(t *testing.T) {
	backend, err := memory.New()
	if err != nil {
		t.Fatal(err)
	}
	api := createAPI("d6MrLT7MwlhtaoQu2b5lWFr", backend)
	apiListener, err := net.Listen("tcp", "localhost:11111")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		log.Println(api.Serve(apiListener))
	}()
	defer func() {
		if err := api.Shutdown(context.TODO()); err != nil {
			log.Println(err)
		}
	}()
	t.Run("Test client flow", func(t *testing.T) {
		c := client.New("http://localhost:11111", "d6MrLT7MwlhtaoQu2b5lWFr")
		if err := c.AddTask("queue", 15, []byte("payload_123")); err != nil {
			t.Fatal(err)
		}
		taskID, payload, err := c.WaitWorkerTask("queue", 10, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if taskID != "1" {
			t.Fatalf("taskID is not equal: %s != %s", taskID, "1")
		}
		if string(payload) != "payload_123" {
			t.Fatalf("payload is not equal: %s != %s", string(payload), "payload_123")
		}
		if err := c.SetTaskReady(taskID, []byte("result_123")); err != nil {
			t.Fatal(err)
		}
		result, err := c.WaitTaskReady(taskID, 10, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(result, []byte("result_123")) {
			t.Fatalf("result is not equal: %s != %s", string(result), "result_123")
		}
	})
}
