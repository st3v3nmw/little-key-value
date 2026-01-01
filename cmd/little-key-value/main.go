package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/st3v3nmw/little-key-value/internal/api"
	"github.com/st3v3nmw/little-key-value/internal/store"
)

func main() {
	log.Print("starting Little Key-Value Store...")

	port := flag.String("port", "", "Port to run the server on")
	workingDir := flag.String("working-dir", "", "Directory for data persistence")
	flag.Parse()

	if *port == "" {
		log.Fatal("--port parameter is required")
	}

	if *workingDir == "" {
		log.Fatal("--working-dir parameter is required")
	}

	ds, err := store.NewDiskStore(*workingDir)
	if err != nil {
		log.Fatalf("failed to create disk store: %v", err)
	}

	server := api.New(ds)

	go func() {
		err = server.Serve(":" + *port)
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	log.Printf("server started on port %s", *port)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM)

	<-quit
	log.Print("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err = server.Shutdown(ctx)
	if err != nil {
		log.Printf("error while shutting down: %v", err)
	}

	server.PrintStats()

	err = ds.Close()
	if err != nil {
		log.Printf("failed to close log: %v", err)
	}

	log.Print("server stopped")
}
