package main

import (
	"cyberflat/stq/backends"
	"cyberflat/stq/backends/memory"
	"errors"
	"fmt"
	"log"
	"os"
)

var (
	ErrUnknownBackend = errors.New("unknown backend")
)

func NewBackend(name string) (backends.Backend, error) {
	switch name {
	case "memory":
		return memory.New()
	default:
		return nil, ErrUnknownBackend
	}
}

func main() {
	log.Println("STQ v1.0")

	backendName := os.Getenv("BACKEND")
	if backendName == "" {
		log.Fatal("BACKEND environment variable is not set")
	}
	// Create a new backend.
	backend, err := NewBackend(backendName)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Backend:", backend.Name())

	// api := gin.Default()
	// api.POST("/:queue", func(c *gin.Context) {
	// 	queue := c.Param("queue")
	// 	payload, err := ioutil.ReadAll(c.Request.Body)
	// 	if err != nil {
	// 		c.AbortWithError(500, err)
	// 		return
	// 	}
	// 	task := backends.Task{
	// 		ID:      uuid.NewString(),
	// 		Payload: payload,
	// 	}
	// 	if err := backend.Put(queue, &task, time.Minute); err != nil {
	// 		c.AbortWithError(500, err)
	// 		return
	// 	}
	// 	c.JSON(200, gin.H{
	// 		"status": "ok",
	// 	})
	// })
	// api.GET("/incomplete/:queue", func(c *gin.Context) {
	// 	queue := c.Param("queue")
	// 	task, err := backend.GetIncomplete(queue)
	// 	if err != nil {
	// 		c.AbortWithError(500, err)
	// 		return
	// 	}
	// 	if task == nil {
	// 		c.AbortWithStatus(404)
	// 		return
	// 	}
	// 	c.JSON(200, task)
	// })
	// api.GET("/complete/:queue/:taskid", func(c *gin.Context) {
	// 	queue := c.Param("queue")
	// 	taskID := c.Param("taskid")
	// 	task, err := backend.GetComplete(queue)
	// 	if err != nil {
	// 		c.AbortWithError(500, err)
	// 		return
	// 	}
	// 	if task == nil {
	// 		c.AbortWithStatus(404)
	// 		return
	// 	}
	// 	c.JSON(200, task)
	// })
}
