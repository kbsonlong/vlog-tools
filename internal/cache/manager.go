package cache

import (
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"
)

type Manager struct {
	dataPath  string
	maxSizeGB int64
	logger    *zap.Logger
	mu        sync.RWMutex
	entries   map[string]*CacheEntry
}

type CacheEntry struct {
	Partition  string
	SizeBytes  int64
	LastAccess time.Time
	CreatedAt  time.Time
}

func NewManager(dataPath string, maxSizeGB int64, logger *zap.Logger) *Manager {
	return &Manager{
		dataPath:  dataPath,
		maxSizeGB: maxSizeGB,
		logger:    logger,
		entries:   make(map[string]*CacheEntry),
	}
}

func (m *Manager) Add(partition string, sizeBytes int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	currentSize := m.getCurrentSize()
	maxSize := m.maxSizeGB * 1024 * 1024 * 1024

	if currentSize+sizeBytes > maxSize {
		if err := m.evictLRU(sizeBytes); err != nil {
			return err
		}
	}

	entry := &CacheEntry{
		Partition:  partition,
		SizeBytes:  sizeBytes,
		LastAccess: time.Now(),
		CreatedAt:  time.Now(),
	}

	m.entries[partition] = entry
	return nil
}

func (m *Manager) Has(partition string) bool {
	m.mu.RLock()
	entry, exists := m.entries[partition]
	m.mu.RUnlock()

	if !exists {
		return false
	}

	// 简单验证目录是否存在
	partitionPath := filepath.Join(m.dataPath, "partitions", partition)
	if _, err := os.Stat(partitionPath); os.IsNotExist(err) {
		m.mu.Lock()
		delete(m.entries, partition)
		m.mu.Unlock()
		return false
	}

	m.mu.Lock()
	entry.LastAccess = time.Now()
	m.mu.Unlock()

	return true
}

func (m *Manager) getCurrentSize() int64 {
	var total int64
	for _, e := range m.entries {
		total += e.SizeBytes
	}
	return total
}

func (m *Manager) evictLRU(requiredBytes int64) error {
	var entriesList []*CacheEntry
	for _, e := range m.entries {
		entriesList = append(entriesList, e)
	}

	sort.Slice(entriesList, func(i, j int) bool {
		return entriesList[i].LastAccess.Before(entriesList[j].LastAccess)
	})

	freedBytes := int64(0)
	for _, entry := range entriesList {
		if freedBytes >= requiredBytes {
			break
		}

		m.logger.Info("Evicting partition", zap.String("partition", entry.Partition))

		partitionPath := filepath.Join(m.dataPath, "partitions", entry.Partition)
		if err := os.RemoveAll(partitionPath); err != nil {
			return err
		}

		delete(m.entries, entry.Partition)
		freedBytes += entry.SizeBytes
	}

	return nil
}

func (m *Manager) Cleanup(retentionDays int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	cutoff := time.Now().AddDate(0, 0, -retentionDays)

	for partition, entry := range m.entries {
		if entry.LastAccess.Before(cutoff) {
			partitionPath := filepath.Join(m.dataPath, "partitions", partition)
			m.logger.Info("Cleaning up old partition", zap.String("partition", partition))

			if err := os.RemoveAll(partitionPath); err != nil {
				m.logger.Error("Failed to cleanup", zap.Error(err))
				continue
			}

			delete(m.entries, partition)
		}
	}

	return nil
}
