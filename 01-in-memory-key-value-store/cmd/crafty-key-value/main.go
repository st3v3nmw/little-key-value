package main

import (
	"fmt"

	"github.com/st3v3nmw/crafty-key-value/internal/api"
)

func main() {
	fmt.Println("Starting Crafty Key-Value Store...")

	server := api.New()
	server.Serve(":8888")
}
