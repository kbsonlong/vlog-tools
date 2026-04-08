package metadata

import (
	"context"
	"encoding/json"
	"time"

	"github.com/vlog-tools/vlog-tools/internal/storage"
	"go.uber.org/zap"
)

type Manager struct {
	storage storage.Storage
	logger  *zap.Logger
}

func NewManager(storage storage.Storage, logger *zap.Logger) *Manager {
	return &Manager{
		storage: storage,
		logger:  logger,
	}
}

type PartitionMap struct {
	Version     int             `json:"version"`
	Partitions  []PartitionInfo `json:"partitions"`
	GeneratedAt time.Time       `json:"generated_at"`
}

type PartitionInfo struct {
	Name       string    `json:"name"`
	Nodes      []string  `json:"nodes"`
	SizeBytes  int64     `json:"size_bytes"`
	FileCount  int       `json:"file_count"`
	ModifiedAt time.Time `json:"modified_at"`
}

func (m *Manager) UpdatePartitionMap(ctx context.Context, partition string, nodes []string) error {
	partitionMap, err := m.LoadPartitionMap(ctx)
	if err != nil {
		partitionMap = &PartitionMap{
			Version:    1,
			Partitions: []PartitionInfo{},
		}
	}

	found := false
	for i, p := range partitionMap.Partitions {
		if p.Name == partition {
			partitionMap.Partitions[i].Nodes = nodes
			partitionMap.Partitions[i].ModifiedAt = time.Now().UTC()
			found = true
			break
		}
	}

	if !found {
		partitionMap.Partitions = append(partitionMap.Partitions, PartitionInfo{
			Name:       partition,
			Nodes:      nodes,
			ModifiedAt: time.Now().UTC(),
		})
	}

	partitionMap.GeneratedAt = time.Now().UTC()

	return m.savePartitionMap(ctx, partitionMap)
}

func (m *Manager) LoadPartitionMap(ctx context.Context) (*PartitionMap, error) {
	data, err := m.storage.GetMetadata(ctx, "partition-map.json")
	if err != nil {
		return nil, err
	}

	var partitionMap PartitionMap
	if err := json.Unmarshal(data, &partitionMap); err != nil {
		return nil, err
	}

	return &partitionMap, nil
}

func (m *Manager) savePartitionMap(ctx context.Context, partitionMap *PartitionMap) error {
	data, err := json.MarshalIndent(partitionMap, "", "  ")
	if err != nil {
		return err
	}
	return m.storage.PutMetadata(ctx, "partition-map.json", data)
}
