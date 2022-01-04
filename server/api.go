package main

import (
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/alexio777/stq/server/backends"
)

func checkAPIKey(r *http.Request, apiKey string) bool {
	if apiKey == "" {
		return true
	}
	return r.Header.Get("X-API-KEY") == apiKey
}

func createAPI(apiKey string, backend backends.Backend) *http.Server {
	mux := http.NewServeMux()
	// POST /task?queue=queuename&timeout=seconds and payload in body
	// return task id
	mux.HandleFunc("/task", func(rw http.ResponseWriter, r *http.Request) {
		if !checkAPIKey(r, apiKey) {
			http.Error(rw, "invalid API key", http.StatusUnauthorized)
			return
		}
		if r.Method != "POST" {
			rw.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		queue := r.URL.Query().Get("queue")
		if queue == "" {
			http.Error(rw, "queue is empty", http.StatusBadRequest)
			return
		}
		timeout, err := strconv.Atoi(r.URL.Query().Get("timeout"))
		if err != nil {
			http.Error(rw, "timeout is empty", http.StatusBadRequest)
			return
		}
		payload, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusBadRequest)
			return
		}
		taskID, err := backend.Put(queue, []byte(payload), time.Second*time.Duration(timeout))
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		rw.Write([]byte(taskID))
	})
	// GET /task/worker?queue=queuename
	// return X-TASK-ID header and payload in body
	mux.HandleFunc("/task/worker", func(rw http.ResponseWriter, r *http.Request) {
		if !checkAPIKey(r, apiKey) {
			http.Error(rw, "invalid API key", http.StatusUnauthorized)
			return
		}
		if r.Method != "GET" {
			rw.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		queue := r.URL.Query().Get("queue")
		if queue == "" {
			http.Error(rw, "queue is empty", http.StatusBadRequest)
			return
		}
		taskID, payload, err := backend.GetNotReady(queue)
		if err != nil {
			if err == backends.ErrQueueNotFound {
				http.Error(rw, "", http.StatusNotFound)
				return
			}
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		rw.Header().Set("X-TASK-ID", taskID)
		rw.Write(payload)
	})
	// POST /task/ready taskid in query and payload in body
	mux.HandleFunc("/task/ready", func(rw http.ResponseWriter, r *http.Request) {
		if !checkAPIKey(r, apiKey) {
			http.Error(rw, "invalid API key", http.StatusUnauthorized)
			return
		}
		if r.Method != "POST" {
			rw.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		taskID := r.URL.Query().Get("taskid")
		if taskID == "" {
			http.Error(rw, "task id is empty", http.StatusBadRequest)
			return
		}
		result, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusBadRequest)
			return
		}
		err = backend.TaskReady(taskID, result)
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
	})
	// GET /task/result?taskid=taskid
	mux.HandleFunc("/task/result", func(rw http.ResponseWriter, r *http.Request) {
		if !checkAPIKey(r, apiKey) {
			http.Error(rw, "invalid API key", http.StatusUnauthorized)
			return
		}
		if r.Method != "GET" {
			rw.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		taskID := r.URL.Query().Get("taskid")
		if taskID == "" {
			http.Error(rw, "task id is empty", http.StatusBadRequest)
			return
		}
		result, err := backend.GetReady(taskID)
		if err != nil {
			if err == backends.ErrTaskNotFoundOrNotReady {
				http.Error(rw, "", http.StatusNotFound)
				return
			}
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		rw.Write(result)
	})
	mux.HandleFunc("/stats", func(rw http.ResponseWriter, r *http.Request) {
		if !checkAPIKey(r, apiKey) {
			http.Error(rw, "invalid API key", http.StatusUnauthorized)
			return
		}
		if r.Method != "GET" {
			rw.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		stats, err := backend.Stats()
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		rw.Write(stats)
	})
	server := &http.Server{Handler: mux}
	return server
}
