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
	flushInterval    = 50 * time.Millisecond
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

// DiskStore provides durable storage that survives restarts.
type DiskStore struct {
	memory *MemoryStore

	workingDir   string
	snapshotPath string
	walPath      string
	wal          *os.File
	walMu        sync.Mutex // serializes access to the WAL

	batchChan chan *batchRequest
	batchDone chan struct{}

	opCount       atomic.Int64
	checkpointing atomic.Bool
	checkpointMu  sync.RWMutex // ensures atomicity between memory and WAL updates

	closeOnce sync.Once
}

// NewDiskStore creates a new persistent store that saves data to the specified directory.
// Any previously saved state is restored on initialization.
func NewDiskStore(workingDir string) (*DiskStore, error) {
	snapshotPath := filepath.Join(workingDir, "snapshot.json")
	logPath := filepath.Join(workingDir, "wal.jsonl")

	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	ds := &DiskStore{
		memory:       NewMemoryStore(),
		workingDir:   workingDir,
		snapshotPath: snapshotPath,
		walPath:      logPath,
		wal:          logFile,
		batchChan:    make(chan *batchRequest, 5*maxBatchSize),
		batchDone:    make(chan struct{}),
	}

	err = ds.loadSnapshot()
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("failed to load snapshot: %w", err)
	}

	err = ds.replayWAL()
	if err != nil {
		logFile.Close()
		return nil, fmt.Errorf("failed to replay WAL: %w", err)
	}

	go ds.batchWriter()

	return ds, nil
}

// loadSnapshot restores the store's state from disk.
func (ds *DiskStore) loadSnapshot() error {
	data, err := os.ReadFile(ds.snapshotPath)
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

	ds.memory.data = snapshot

	return nil
}

// saveSnapshot captures the current state to disk.
func (ds *DiskStore) saveSnapshot() error {
	ds.memory.mu.RLock()
	data, err := json.Marshal(ds.memory.data)
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}
	ds.memory.mu.RUnlock()

	err = os.WriteFile(ds.snapshotPath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write snapshot: %w", err)
	}

	return nil
}

// replayWAL recovers operations from the WAL.
func (ds *DiskStore) replayWAL() error {
	f, err := os.Open(ds.walPath)
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
			return fmt.Errorf("corrupted WAL entry at line %d: %w", lineNum, err)
		}

		switch entry.Op {
		case "set":
			ds.memory.Set(entry.Key, entry.Value)
		case "delete":
			ds.memory.Delete(entry.Key)
		case "clear":
			ds.memory.Clear()
		default:
			return fmt.Errorf("unknown operation %q at line %d", entry.Op, lineNum)
		}
	}

	err = scanner.Err()
	if err != nil {
		return fmt.Errorf("failed to read WAL: %w", err)
	}

	return nil
}

// batchWriter runs in the background to batch WAL writes and fsync.
func (ds *DiskStore) batchWriter() {
	batch := make([]*batchRequest, 0, maxBatchSize)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for {
		select {
		case req, ok := <-ds.batchChan:
			if !ok {
				if len(batch) > 0 {
					ds.flushBatch(batch)
				}

				close(ds.batchDone)

				return
			}

			batch = append(batch, req)

			if len(batch) >= maxBatchSize {
				ds.flushBatch(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				ds.flushBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

// flushBatch writes all entries in the batch and performs a single fsync.
func (ds *DiskStore) flushBatch(batch []*batchRequest) {
	ds.walMu.Lock()
	defer ds.walMu.Unlock()

	var writeErr error
	for _, req := range batch {
		data, err := json.Marshal(req.entry)
		if err != nil {
			writeErr = fmt.Errorf("failed to marshal WAL entry: %w", err)
			break
		}

		_, err = ds.wal.Write(append(data, '\n'))
		if err != nil {
			writeErr = fmt.Errorf("failed to write to WAL: %w", err)
			break
		}
	}

	var syncErr error
	if writeErr == nil {
		syncErr = ds.wal.Sync()
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
func (ds *DiskStore) checkpoint() error {
	defer ds.checkpointing.Store(false)

	ds.checkpointMu.Lock()
	defer ds.checkpointMu.Unlock()

	ds.walMu.Lock()
	defer ds.walMu.Unlock()

	err := ds.saveSnapshot()
	if err != nil {
		return fmt.Errorf("checkpoint failed: %w", err)
	}

	err = ds.wal.Close()
	if err != nil {
		return fmt.Errorf("failed to close WAL during checkpoint: %w", err)
	}

	err = os.Truncate(ds.walPath, 0)
	if err != nil {
		return fmt.Errorf("failed to truncate WAL during checkpoint: %w", err)
	}

	ds.wal, err = os.OpenFile(ds.walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen WAL during checkpoint: %w", err)
	}

	ds.opCount.Store(0)

	return nil
}

// appendToWAL records an operation to the WAL.
func (ds *DiskStore) appendToWAL(entry LogEntry) error {
	req := &batchRequest{entry: entry, errChan: make(chan error, 1)}
	ds.batchChan <- req

	err := <-req.errChan
	if err != nil {
		return err
	}

	newCount := ds.opCount.Add(1)
	if newCount%opsPerCheckpoint == 0 && ds.checkpointing.CompareAndSwap(false, true) {
		go func() {
			err := ds.checkpoint()
			if err != nil {
				log.Printf("checkpoint failed: %v", err)
			}
		}()
	}

	return nil
}

// Set adds or updates a key-value pair in the store.
func (ds *DiskStore) Set(key, value string) error {
	ds.checkpointMu.RLock()
	defer ds.checkpointMu.RUnlock()

	err := ds.appendToWAL(LogEntry{Op: "set", Key: key, Value: value})
	if err != nil {
		return err
	}

	return ds.memory.Set(key, value)
}

// Get retrieves the value associated with a given key.
func (ds *DiskStore) Get(key string) (string, error) {
	return ds.memory.Get(key)
}

// Delete removes a key-value pair from the store.
func (ds *DiskStore) Delete(key string) error {
	ds.checkpointMu.RLock()
	defer ds.checkpointMu.RUnlock()

	err := ds.appendToWAL(LogEntry{Op: "delete", Key: key})
	if err != nil {
		return err
	}

	return ds.memory.Delete(key)
}

// Clear removes all key-value pairs from the store.
func (ds *DiskStore) Clear() error {
	ds.checkpointMu.RLock()
	defer ds.checkpointMu.RUnlock()

	err := ds.appendToWAL(LogEntry{Op: "clear"})
	if err != nil {
		return err
	}

	return ds.memory.Clear()
}

// Close performs a clean shutdown of the store.
func (ds *DiskStore) Close() error {
	var closeErr error

	ds.closeOnce.Do(func() {
		close(ds.batchChan)
		<-ds.batchDone

		ds.checkpointMu.Lock()
		defer ds.checkpointMu.Unlock()

		err := ds.saveSnapshot()
		if err != nil {
			log.Printf("failed to save snapshot: %v", err)
			closeErr = err
		}

		ds.walMu.Lock()
		err = ds.wal.Close()
		ds.walMu.Unlock()
		if err != nil {
			closeErr = err
			return
		}

		err = os.Truncate(ds.walPath, 0)
		if err != nil {
			log.Printf("failed to truncate WAL: %v", err)
		}
	})

	return closeErr
}
