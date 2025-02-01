package main

import (
	"fmt"

	"github.com/st3v3nmw/little-key-value/internal/api"
)

func main() {
	fmt.Println("Starting Little Key-Value Store...")

	server := api.New()
	server.Serve(":8888")
}
