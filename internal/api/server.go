package api

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/st3v3nmw/little-key-value/internal/storage"
)

// Storage defines the interface for a generic key-value storage system
type Storage interface {
	// Set adds or updates a key-value pair in the storage
	Set(key string, value string) error

	// Get retrieves the value associated with a given key
	Get(key string) (string, error)

	// Delete removes a key-value pair from the storage
	Delete(key string) error

	// Clear removes all key-value pairs from the storage
	Clear() error
}

// Server represents the key-value server
type Server struct {
	storage Storage
}

// New creates a new instance of the Server
func New() *Server {
	return &Server{storage: storage.NewMapStorage()}
}

// Serve starts the HTTP server and handles key-value store operations
func (s *Server) Serve(addr string) error {
	http.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path[len("/kv/"):]
		if len(key) == 0 {
			http.Error(w, "key cannot be empty", http.StatusBadRequest)
			return
		}

		switch r.Method {
		case http.MethodPut:
			s.set(w, r)
		case http.MethodGet:
			s.get(w, r)
		case http.MethodDelete:
			s.delete(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	http.HandleFunc("/clear", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			s.clear(w, r)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	return http.ListenAndServe(addr, nil)
}

// set handles the HTTP PUT request for setting a key-value pair
func (s *Server) set(w http.ResponseWriter, r *http.Request) {
	value, err := io.ReadAll(r.Body)
	if err != nil {
		msg := fmt.Sprintf("unable to read request body: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}
	if len(value) == 0 {
		http.Error(w, "value cannot be empty", http.StatusBadRequest)
		return
	}

	key := r.URL.Path[len("/kv/"):]
	err = s.storage.Set(key, string(value))
	if err != nil {
		msg := fmt.Sprintf("unable to set key: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// get handles the HTTP GET request for retrieving a key-value pair
func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/kv/"):]
	value, err := s.storage.Get(key)
	if err != nil {
		if errors.Is(err, &storage.NotFoundError{}) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		msg := fmt.Sprintf("unable to get key: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	w.Write([]byte(value))
}

// delete handles the HTTP DELETE request for deleting a key-value pair
func (s *Server) delete(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Path[len("/kv/"):]
	err := s.storage.Delete(key)
	if err != nil {
		msg := fmt.Sprintf("unable to delete key: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// clear handles the HTTP POST request for clearing the storage
func (s *Server) clear(w http.ResponseWriter, _ *http.Request) {
	err := s.storage.Clear()
	if err != nil {
		msg := fmt.Sprintf("unable to clear storage: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
