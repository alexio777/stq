package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/alexio777/stq/server/backends"
	"github.com/alexio777/stq/server/backends/memory"
)

func Test_API(t *testing.T) {
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
	t.Run("Test API flow", func(t *testing.T) {
		// add task
		req, err := http.NewRequest("POST",
			"http://localhost:11111/task?queue=queue&timeout=15",
			bytes.NewBufferString("payload_123"))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-API-KEY", "d6MrLT7MwlhtaoQu2b5lWFr")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := ioutil.ReadAll(resp.Body)
			t.Fatalf("unexpected status code: %d, body: %s", resp.StatusCode, body)
		}
		taskIDRaw, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if taskIDRaw == nil {
			t.Fatal("taskID is nil")
		}
		if string(taskIDRaw) != "1" {
			t.Fatalf("taskIDRaw is not equal: %s != %s", string(taskIDRaw), "1")
		}
		// worker wait task
		var taskID string
		repeats := 30
		for repeats > 0 {
			req, err = http.NewRequest("GET", "http://localhost:11111/task/worker?queue=queue", nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("X-API-KEY", "d6MrLT7MwlhtaoQu2b5lWFr")
			resp, err = http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode == http.StatusOK {
				taskID = resp.Header.Get("X-TASK-ID")
				if taskID != "1" {
					t.Fatalf("taskID is not equal: %s != %s", taskID, "1")
				}
				payload, err := ioutil.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}
				if string(payload) != "payload_123" {
					t.Fatalf("payload is not equal: %s != %s", string(payload), "payload")
				}
				break
			}
			time.Sleep(time.Second)
			repeats--
		}
		if repeats == 0 {
			t.Fatalf("task is not ready")
		}
		// worker finished task
		req, err = http.NewRequest("POST", "http://localhost:11111/task/ready?taskid="+taskID,
			bytes.NewBufferString("result_123"))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-API-KEY", "d6MrLT7MwlhtaoQu2b5lWFr")
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := ioutil.ReadAll(resp.Body)
			t.Fatalf("unexpected status code: %d, body: %s", resp.StatusCode, body)
		}
		// get ready task
		req, err = http.NewRequest("GET", "http://localhost:11111/task/result?taskid="+taskID, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-API-KEY", "d6MrLT7MwlhtaoQu2b5lWFr")
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := ioutil.ReadAll(resp.Body)
			t.Fatalf("unexpected status code: %d, body: %s", resp.StatusCode, body)
		}
		result, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(result) != "result_123" {
			t.Fatalf("result is not equal: %s != %s", string(result), "result")
		}
		// check stats
		req, err = http.NewRequest("GET", "http://localhost:11111/stats", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-API-KEY", "d6MrLT7MwlhtaoQu2b5lWFr")
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := ioutil.ReadAll(resp.Body)
			t.Fatalf("unexpected status code: %d, body: %s", resp.StatusCode, body)
		}
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		var stats map[string]backends.Stats
		err = json.Unmarshal(data, &stats)
		if err != nil {
			t.Fatal(err)
		}
		if stats["queue"].WaitLength != 0 && stats["queue"].WorkLength != 0 && stats["queue"].ReadyLength != 1 {
			t.Fatalf("stats is not equal zero: %v", stats)
		}
	})
}
