package store

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxBatchSize     = 100
	flushInterval    = 10 * time.Millisecond
	opsPerCheckpoint = 1_000
)

// LogEntry represents a single operation in the log.
type LogEntry struct {
	Op    string `json:"op"` // "set", "delete", "clear"
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// batchRequest represents a pending write request waiting for fsync.
type batchRequest struct {
	entry   LogEntry
	errChan chan error
}

// PersistentStore provides durable storage that survives restarts.
type PersistentStore struct {
	mem *MapStore

	workingDir   string
	snapshotPath string
	walPath      string
	wal          *os.File
	ioMu         sync.Mutex // serializes access to the WAL file descriptor

	batchChan chan *batchRequest
	batchDone chan struct{}

	opCount       atomic.Int64
	checkpointing atomic.Bool
	checkpointMu  sync.RWMutex // ensures atomicity between memory and WAL updates

	closeOnce sync.Once
}

// NewPersistentStore creates a new persistent store that saves data to the specified directory.
// Any previously saved state is restored on initialization.
func NewPersistentStore(workingDir string) (*PersistentStore, error) {
	snapshotPath := filepath.Join(workingDir, "snapshot.json")
	logPath := filepath.Join(workingDir, "wal.jsonl")

	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	ps := &PersistentStore{
		mem:          NewMapStore(),
		workingDir:   workingDir,
		snapshotPath: snapshotPath,
		walPath:      logPath,
		wal:          logFile,
		batchChan:    make(chan *batchRequest, 5*maxBatchSize),
		batchDone:    make(chan struct{}),
	}

	err = ps.loadSnapshot()
	if err != nil {
		logFile.Close()

		return nil, fmt.Errorf("failed to load snapshot: %w", err)
	}

	err = ps.replayWAL()
	if err != nil {
		logFile.Close()

		return nil, fmt.Errorf("failed to replay WAL: %w", err)
	}

	go ps.batchWriter()

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

	ps.mem.data = snapshot

	return nil
}

// saveSnapshot captures the current state to disk.
func (ps *PersistentStore) saveSnapshot() error {
	ps.mem.mu.RLock()
	data, err := json.MarshalIndent(ps.mem.data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}
	ps.mem.mu.RUnlock()

	err = os.WriteFile(ps.snapshotPath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}

	return nil
}

// replayWAL recovers operations from the WAL.
func (ps *PersistentStore) replayWAL() error {
	f, err := os.Open(ps.walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return fmt.Errorf("failed to open WAL for replay: %w", err)
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
			log.Printf("skipping corrupted WAL line %d: %v", lineNum, err)
			continue
		}

		switch entry.Op {
		case "set":
			ps.mem.Set(entry.Key, entry.Value)
		case "delete":
			ps.mem.Delete(entry.Key)
		case "clear":
			ps.mem.Clear()
		default:
			log.Printf("unknown operation %q on line %d", entry.Op, lineNum)
		}
	}

	err = scanner.Err()
	if err != nil {
		return fmt.Errorf("failed to read WAL: %w", err)
	}

	return nil
}

// batchWriter runs in the background to batch WAL writes and fsync.
func (ps *PersistentStore) batchWriter() {
	batch := make([]*batchRequest, 0, maxBatchSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case req, ok := <-ps.batchChan:
			if !ok {
				if len(batch) > 0 {
					ps.flushBatch(batch)
				}

				close(ps.batchDone)

				return
			}

			batch = append(batch, req)

			if len(batch) >= maxBatchSize {
				ps.flushBatch(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				ps.flushBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

// flushBatch writes all entries in the batch and performs a single fsync.
func (ps *PersistentStore) flushBatch(batch []*batchRequest) {
	ps.ioMu.Lock()
	defer ps.ioMu.Unlock()

	var writeErr error
	for _, req := range batch {
		data, err := json.Marshal(req.entry)
		if err != nil {
			writeErr = fmt.Errorf("failed to marshal WAL entry: %w", err)
			break
		}

		_, err = ps.wal.Write(append(data, '\n'))
		if err != nil {
			writeErr = fmt.Errorf("failed to write to WAL: %w", err)
			break
		}
	}

	var syncErr error
	if writeErr == nil {
		syncErr = ps.wal.Sync()
		if syncErr != nil {
			syncErr = fmt.Errorf("failed to sync WAL: %w", syncErr)
		}
	}

	finalErr := writeErr
	if finalErr == nil {
		finalErr = syncErr
	}

	for _, req := range batch {
		req.errChan <- finalErr
	}
}

// checkpoint creates a snapshot and truncates the WAL.
func (ps *PersistentStore) checkpoint() error {
	defer ps.checkpointing.Store(false)

	ps.checkpointMu.Lock()
	defer ps.checkpointMu.Unlock()

	ps.ioMu.Lock()
	defer ps.ioMu.Unlock()

	err := ps.saveSnapshot()
	if err != nil {
		return fmt.Errorf("checkpoint failed: %w", err)
	}

	err = ps.wal.Close()
	if err != nil {
		return fmt.Errorf("failed to close WAL during checkpoint: %w", err)
	}

	err = os.Truncate(ps.walPath, 0)
	if err != nil {
		return fmt.Errorf("failed to truncate WAL during checkpoint: %w", err)
	}

	ps.wal, err = os.OpenFile(ps.walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen WAL during checkpoint: %w", err)
	}

	ps.opCount.Store(0)

	return nil
}

// appendToWAL records an operation to the WAL.
func (ps *PersistentStore) appendToWAL(entry LogEntry) error {
	req := &batchRequest{entry: entry, errChan: make(chan error, 1)}
	ps.batchChan <- req

	err := <-req.errChan
	if err != nil {
		return err
	}

	newCount := ps.opCount.Add(1)
	if newCount%opsPerCheckpoint == 0 && ps.checkpointing.CompareAndSwap(false, true) {
		go func() {
			err := ps.checkpoint()
			if err != nil {
				log.Printf("checkpoint failed: %v", err)
			}
		}()
	}

	return nil
}

// Set adds or updates a key-value pair in the store.
func (ps *PersistentStore) Set(key, value string) error {
	ps.checkpointMu.RLock()
	defer ps.checkpointMu.RUnlock()

	err := ps.appendToWAL(LogEntry{Op: "set", Key: key, Value: value})
	if err != nil {
		return err
	}

	return ps.mem.Set(key, value)
}

// Get retrieves the value associated with a given key.
func (ps *PersistentStore) Get(key string) (string, error) {
	return ps.mem.Get(key)
}

// Delete removes a key-value pair from the store.
func (ps *PersistentStore) Delete(key string) error {
	ps.checkpointMu.RLock()
	defer ps.checkpointMu.RUnlock()

	err := ps.appendToWAL(LogEntry{Op: "delete", Key: key})
	if err != nil {
		return err
	}

	return ps.mem.Delete(key)
}

// Clear removes all key-value pairs from the store.
func (ps *PersistentStore) Clear() error {
	ps.checkpointMu.RLock()
	defer ps.checkpointMu.RUnlock()

	err := ps.appendToWAL(LogEntry{Op: "clear"})
	if err != nil {
		return err
	}

	return ps.mem.Clear()
}

// Close performs a clean shutdown of the store.
func (ps *PersistentStore) Close() error {
	var closeErr error

	ps.closeOnce.Do(func() {
		close(ps.batchChan)
		<-ps.batchDone

		ps.checkpointMu.Lock()
		defer ps.checkpointMu.Unlock()

		err := ps.saveSnapshot()
		if err != nil {
			log.Printf("failed to save snapshot: %v", err)
			closeErr = err
		}

		ps.ioMu.Lock()
		err = ps.wal.Close()
		ps.ioMu.Unlock()
		if err != nil {
			closeErr = err
			return
		}

		err = os.Truncate(ps.walPath, 0)
		if err != nil {
			log.Printf("failed to truncate WAL: %v", err)
		}
	})

	return closeErr
}
