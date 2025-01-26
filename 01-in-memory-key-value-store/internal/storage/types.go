package storage

import "fmt"

// Storage defines the interface for a generic key-value storage system
type Storage interface {
	// Set adds or updates a key-value pair in the storage
	Set(key string, value string) error

	// Get retrieves the value associated with a given key
	Get(key string) (string, error)

	// Delete removes a key-value pair from the storage
	Delete(key string) error
}

// NotFoundError is a custom error type for when a key is not found
// It implements the standard error interface
type NotFoundError struct{}

// Error returns a string representation of the error
func (e *NotFoundError) Error() string {
	return fmt.Sprintf("key not found")
}
