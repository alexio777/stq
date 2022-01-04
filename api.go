package main

import (
	"cyberflat/stq/backends"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"
)

func checkAPIKey(r *http.Request, apiKey string) bool {
	if apiKey == "" {
		return true
	}
	return r.Header.Get("X-API-KEY") == apiKey
}

func createAPI(listen string, apiKey string, backend backends.Backend) *http.Server {
	mux := http.NewServeMux()
	// POST /add/task?queue=queuename&timeout=seconds and payload in body
	// return task id
	mux.HandleFunc("/add/task", func(rw http.ResponseWriter, r *http.Request) {
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
	// GET /worker/task?queue=queuename
	// return X-TASK-ID header and payload in body
	mux.HandleFunc("/worker/task", func(rw http.ResponseWriter, r *http.Request) {
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
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		rw.Header().Set("X-TASK-ID", taskID)
		rw.Write(payload)
	})
	// POST /task/ready with X-TASK-ID header and payload in body
	mux.HandleFunc("/task/ready", func(rw http.ResponseWriter, r *http.Request) {
		if !checkAPIKey(r, apiKey) {
			http.Error(rw, "invalid API key", http.StatusUnauthorized)
			return
		}
		if r.Method != "POST" {
			rw.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		taskID := r.Header.Get("X-TASK-ID")
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
	mux.HandleFunc("/get/ready/task", func(rw http.ResponseWriter, r *http.Request) {
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
			http.Error(rw, err.Error(), http.StatusInternalServerError)
			return
		}
		rw.Write(result)
	})
	server := &http.Server{Addr: listen, Handler: mux}
	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Println(err)
		}
	}()
	return server
}
