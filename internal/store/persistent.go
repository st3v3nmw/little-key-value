package store

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// LogEntry represents a single operation in the log.
type LogEntry struct {
	Op    string `json:"op"` // "set", "delete", "clear"
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// PersistentStore provides durable storage that survives restarts.
type PersistentStore struct {
	store *MapStore

	workingDir   string
	snapshotPath string
	logPath      string
	logFile      *os.File
	mu           sync.Mutex // protects file I/O
}

// NewPersistentStore creates a new persistent store that saves data to the specified directory.
// Any previously saved state is restored on initialization.
func NewPersistentStore(workingDir string) (*PersistentStore, error) {
	_, err := os.Stat(workingDir)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("working directory %q does not exist", workingDir)
	}

	snapshotPath := filepath.Join(workingDir, "snapshot.json")
	logPath := filepath.Join(workingDir, "wal.jsonl")

	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	ps := &PersistentStore{
		store:        NewMapStore(),
		workingDir:   workingDir,
		snapshotPath: snapshotPath,
		logPath:      logPath,
		logFile:      logFile,
	}

	err = ps.loadSnapshot()
	if err != nil {
		logFile.Close()

		return nil, fmt.Errorf("failed to load snapshot: %w", err)
	}

	err = ps.replay()
	if err != nil {
		logFile.Close()

		return nil, fmt.Errorf("failed to replay log: %w", err)
	}

	return ps, nil
}

// loadSnapshot restores the store's state from disk.
func (ps *PersistentStore) loadSnapshot() error {
	data, err := os.ReadFile(ps.snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No snapshot yet, start with empty store
			return nil
		}

		return fmt.Errorf("failed to read snapshot: %w", err)
	}

	var snapshot map[string]string
	err = json.Unmarshal(data, &snapshot)
	if err != nil {
		return fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	ps.store.mu.Lock()
	defer ps.store.mu.Unlock()

	ps.store.data = snapshot

	return nil
}

// saveSnapshot captures the current state to disk.
func (ps *PersistentStore) saveSnapshot() error {
	ps.store.mu.RLock()
	data, err := json.MarshalIndent(ps.store.data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}
	ps.store.mu.RUnlock()

	err = os.WriteFile(ps.snapshotPath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}

	return nil
}

// replay recovers operations from the log.
func (ps *PersistentStore) replay() error {
	f, err := os.Open(ps.logPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No log file yet, start with empty file
			return nil
		}

		return fmt.Errorf("failed to open log for replay: %w", err)
	}
	defer f.Close()

	lineNum := 0
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		if line == "" {
			continue
		}

		var entry LogEntry
		err := json.Unmarshal([]byte(line), &entry)
		if err != nil {
			log.Printf("skipping corrupted log line %d: %v", lineNum, err)
			continue
		}

		switch entry.Op {
		case "set":
			ps.store.Set(entry.Key, entry.Value)
		case "delete":
			ps.store.Delete(entry.Key)
		case "clear":
			ps.store.Clear()
		default:
			log.Printf("unknown operation %q on line %d", entry.Op, lineNum)
		}
	}

	err = scanner.Err()
	if err != nil {
		return fmt.Errorf("failed to read log: %w", err)
	}

	return nil
}

// appendLog records an operation to the log.
func (ps *PersistentStore) appendLog(entry LogEntry) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal log entry: %w", err)
	}

	_, err = ps.logFile.Write(append(data, '\n'))
	if err != nil {
		return fmt.Errorf("failed to write to log: %w", err)
	}

	return nil
}

// Set adds or updates a key-value pair in the store.
func (ps *PersistentStore) Set(key, value string) error {
	err := ps.appendLog(LogEntry{Op: "set", Key: key, Value: value})
	if err != nil {
		return err
	}

	return ps.store.Set(key, value)
}

// Get retrieves the value associated with a given key.
func (ps *PersistentStore) Get(key string) (string, error) {
	return ps.store.Get(key)
}

// Delete removes a key-value pair from the store.
func (ps *PersistentStore) Delete(key string) error {
	err := ps.appendLog(LogEntry{Op: "delete", Key: key})
	if err != nil {
		return err
	}

	return ps.store.Delete(key)
}

// Clear removes all key-value pairs from the store.
func (ps *PersistentStore) Clear() error {
	err := ps.appendLog(LogEntry{Op: "clear"})
	if err != nil {
		return err
	}

	return ps.store.Clear()
}

// Close performs a clean shutdown of the store.
func (ps *PersistentStore) Close() error {
	err := ps.saveSnapshot()
	if err != nil {
		log.Printf("failed to save snapshot: %v", err)
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	err = ps.logFile.Close()
	if err != nil {
		return err
	}

	err = os.Truncate(ps.logPath, 0)
	if err != nil {
		log.Printf("failed to truncate WAL: %v", err)
	}

	return nil
}
