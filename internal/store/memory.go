package store

import (
	"sync"
)

// NotFoundError is a custom error type for when a key is not found.
type NotFoundError struct{}

// Error returns a string representation of the error.
func (e *NotFoundError) Error() string {
	return "key not found"
}

// MemoryStore is a simple in-memory implementation of the Store interface.
// It uses a map to store key-value pairs.
type MemoryStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewMemoryStore creates a new instance of MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: map[string]string{}}
}

// Set adds or updates a key-value pair in the store.
func (m *MemoryStore) Set(key string, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = value

	return nil
}

// Get retrieves the value associated with a given key.
func (m *MemoryStore) Get(key string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, ok := m.data[key]
	if !ok {
		return "", &NotFoundError{}
	}

	return value, nil
}

// Delete removes a key-value pair from the store.
func (m *MemoryStore) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, key)

	return nil
}

// Clear removes all key-value pairs from the store.
func (m *MemoryStore) Clear() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	clear(m.data)

	return nil
}

// Close performs a clean shutdown of the store.
func (m *MemoryStore) Close() error {
	return nil
}
