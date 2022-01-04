package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/alexio777/stq/server/backends/memory"

	"github.com/alexio777/stq/server/backends"
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
	log.Println("STQ v1.1")

	backendName := os.Getenv("BACKEND")
	if backendName == "" {
		log.Fatal("BACKEND environment variable is not set")
	}
	backend, err := NewBackend(backendName)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Backend:", backend.Name())

	listen := os.Getenv("LISTEN")
	if listen == "" {
		log.Fatal("LISTEN environment variable is not set")
	}
	apiKey := os.Getenv("APIKEY")
	if apiKey == "" {
		log.Fatal("APIKEY environment variable is not set")
	}
	api := createAPI(apiKey, backend)
	apiListener, err := net.Listen("tcp", listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(api.Serve(apiListener))
}
