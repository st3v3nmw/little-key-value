package store

import (
	"sync"
)

// MapStore is a simple in-memory implementation of the Store interface.
// It uses a map to store key-value pairs.
type MapStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewMapStore creates a new instance of MapStore.
func NewMapStore() *MapStore {
	return &MapStore{data: map[string]string{}}
}

// Set adds or updates a key-value pair in the store.
func (m *MapStore) Set(key string, value string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = value

	return nil
}

// Get retrieves the value associated with a given key.
func (m *MapStore) Get(key string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, ok := m.data[key]
	if !ok {
		return "", &NotFoundError{}
	}

	return value, nil
}

// Delete removes a key-value pair from the store.
func (m *MapStore) Delete(key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, key)

	return nil
}

// Clear removes all key-value pairs from the store.
func (m *MapStore) Clear() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	clear(m.data)

	return nil
}

// Close performs a clean shutdown of the store.
func (m *MapStore) Close() error {
	return nil
}
