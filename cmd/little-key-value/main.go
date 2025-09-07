package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/st3v3nmw/little-key-value/internal/api"
)

func main() {
	log.SetFlags(0)

	port := flag.String("port", "", "Port to run the server on")
	flag.Parse()

	if *port == "" {
		log.Fatalf("Error: --port parameter is required")
	}

	fmt.Println("Starting Little Key-Value Store...")

	server := api.New()
	err := server.Serve(":" + *port)
	if err != nil {
		log.Fatal(err)
	}
}
