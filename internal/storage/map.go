package storage

import (
	"sync"
)

// MapStorage is a simple in-memory implementation of the Storage interface
// It uses a map to store key-value pairs
type MapStorage struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewMapStorage creates a new instance of MapStorage
func NewMapStorage() *MapStorage {
	return &MapStorage{
		data: map[string]string{},
	}
}

// Set adds or updates a key-value pair in the storage
func (m *MapStorage) Set(key string, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = value
	return nil
}

// Get retrieves the value associated with a given key
func (m *MapStorage) Get(key string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, ok := m.data[key]
	if !ok {
		return "", &NotFoundError{}
	}
	return value, nil
}

// Delete removes a key-value pair from the storage
func (m *MapStorage) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, key)
	return nil
}

// Clear removes all key-value pairs from the storage
func (m *MapStorage) Clear() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	clear(m.data)
	return nil
}
