package main

import (
	"fmt"
	"log"

	"github.com/st3v3nmw/little-key-value/internal/api"
)

func main() {
	fmt.Println("Starting Little Key-Value Store...")

	server := api.New()
	err := server.Serve(":8888")
	if err != nil {
		log.Fatal(err)
	}
}
