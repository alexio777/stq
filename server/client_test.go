package main

import (
	"context"
	"cyberflat/stq/client"
	"cyberflat/stq/server/backends/memory"
	"log"
	"net"
	"testing"
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
	})
}
