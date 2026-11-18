package generator

import (
	"context"
	"fmt"
	"shortly/helpers"
	"strconv"
	"strings"

	redis "github.com/redis/go-redis/v9"
)

const (
	WorkerBits = 10 // pod id
	RegionBits = 2
	MaxWorker  = (uint64(1) << WorkerBits) - 1 // 1023
	MaxRegion  = (uint64(1) << RegionBits) - 1 // 3

	SafetyCushion = uint64(1_000_000) // Skip margin on crash restart
)

type WorkerIdentity struct {
	RegionID uint64
	WorkerID uint64
}

// GetWorkerIdentity parses the environment variables injected by Kubernetes.
func GetWorkerIdentity() (*WorkerIdentity, error) {
	// Parse Region ID
	regionStr := helpers.GetEnv("REGION_ID")
	if regionStr == "" {
		regionStr = "0" // Default for local testing
	}
	regionID, err := strconv.ParseUint(regionStr, 10, 64)
	if err != nil || regionID > MaxRegion {
		return nil, fmt.Errorf("invalid REGION_ID (max %d): %v", MaxRegion, err)
	}

	// Parse Pod Ordinal from StatefulSet POD_NAME (e.g., shortener-app-5 -> 5)
	podName := helpers.GetEnv("POD_NAME")
	var workerID uint64 = 0
	if podName != "" {
		parts := strings.Split(podName, "-")
		workerID, err = strconv.ParseUint(parts[len(parts)-1], 10, 64)
		if err != nil || workerID > MaxWorker {
			return nil, fmt.Errorf("invalid worker index from POD_NAME '%s' (max %d): %v", podName, MaxWorker, err)
		}
	}

	return &WorkerIdentity{
		RegionID: regionID,
		WorkerID: workerID,
	}, nil
}

// StateStore handles reading/persisting sequence checkpoints on pod startup/shutdown.
// Each channel keeps its own sequence so SMS and RCS/WA advance independently.
type StateStore interface {
	LoadInitialSequence(ctx context.Context, regionID, workerID, channel uint64) (uint64, error)
	SaveCheckpoint(ctx context.Context, regionID, workerID, channel, seq uint64) error
}

type RedisStateStore struct {
	client *redis.Client
}

func NewRedisStateStore(client *redis.Client) *RedisStateStore {
	return &RedisStateStore{client: client}
}

func (s *RedisStateStore) key(regionID, workerID, channel uint64) string {
	return fmt.Sprintf("shortener:checkpoint:region:%d:worker:%d:channel:%d", regionID, workerID, channel)
}

func (s *RedisStateStore) LoadInitialSequence(ctx context.Context, regionID, workerID, channel uint64) (uint64, error) {
	val, err := s.client.Get(ctx, s.key(regionID, workerID, channel)).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	lastSeq, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return 0, err
	}

	// Add safety margin so any un-persisted, in-flight sequences before crash are skipped.
	return lastSeq + SafetyCushion, nil
}

func (s *RedisStateStore) SaveCheckpoint(ctx context.Context, regionID, workerID, channel, seq uint64) error {
	return s.client.Set(ctx, s.key(regionID, workerID, channel), strconv.FormatUint(seq, 10), 0).Err()
}
