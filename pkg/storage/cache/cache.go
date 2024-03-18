package cache

import (
	"context"
	"github.com/logrange/linker"
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	"github.com/solarisdb/solaris/golibs/container/lru"
	"github.com/solarisdb/solaris/pkg/storage"
	"github.com/solarisdb/solaris/pkg/storage/logfs"
)

type (
	// LogsChunksMetaStorage combines storage.Logs and
	// logfs.LogsMetaStorage interfaces
	LogsChunksMetaStorage interface {
		logfs.LogsMetaStorage
		storage.Logs
	}

	// CachedStorage wraps LogsChunksMetaStorage
	// with caches for logs and chunks
	CachedStorage struct {
		storage        LogsChunksMetaStorage
		logsCache      *lru.Cache[string, *solaris.Log]
		chunksCache    *lru.Cache[string, []logfs.ChunkInfo]
		lastChunkCache *lru.Cache[string, logfs.ChunkInfo]
	}
)

const cacheSize = 1000

// NewCachedStorage wraps LogsChunksMetaStorage into cache
func NewCachedStorage(storage LogsChunksMetaStorage) *CachedStorage {
	cache := &CachedStorage{storage: storage}
	cache.logsCache, _ = lru.NewCache(cacheSize, func(logID string) (*solaris.Log, error) {
		return storage.GetLogByID(context.Background(), logID)
	}, nil)
	cache.chunksCache, _ = lru.NewCache(cacheSize, func(logID string) ([]logfs.ChunkInfo, error) {
		return storage.GetChunks(context.Background(), logID)
	}, nil)
	cache.lastChunkCache, _ = lru.NewCache(cacheSize, func(logID string) (logfs.ChunkInfo, error) {
		return storage.GetLastChunk(context.Background(), logID)
	}, nil)
	return cache
}

// Init implements linker.Initializer
func (s *CachedStorage) Init(ctx context.Context) error {
	if init, ok := s.storage.(linker.Initializer); ok {
		return init.Init(ctx)
	}
	return nil
}

// Shutdown implements linker.Shutdowner
func (s *CachedStorage) Shutdown() {
	if shut, ok := s.storage.(linker.Shutdowner); ok {
		shut.Shutdown()
	}
}

// CreateLog implements storage.Logs
func (s *CachedStorage) CreateLog(ctx context.Context, log *solaris.Log) (*solaris.Log, error) {
	return s.storage.CreateLog(ctx, log)
}

// GetLogByID implements storage.Logs
func (s *CachedStorage) GetLogByID(ctx context.Context, id string) (*solaris.Log, error) {
	return s.logsCache.GetOrCreate(id)
}

// UpdateLog implements storage.Logs
func (s *CachedStorage) UpdateLog(ctx context.Context, log *solaris.Log) (*solaris.Log, error) {
	l, err := s.storage.UpdateLog(ctx, log)
	if err != nil {
		return nil, err
	}
	s.logsCache.Remove(log.ID)
	return l, err
}

// QueryLogs implements storage.Logs
func (s *CachedStorage) QueryLogs(ctx context.Context, qr storage.QueryLogsRequest) (*solaris.QueryLogsResult, error) {
	return s.storage.QueryLogs(ctx, qr)
}

// DeleteLogs implements storage.Logs
func (s *CachedStorage) DeleteLogs(ctx context.Context, request storage.DeleteLogsRequest) (*solaris.DeleteLogsResult, error) {
	dr, err := s.storage.DeleteLogs(ctx, request)
	if err != nil {
		return nil, err
	}
	for _, id := range dr.DeletedIDs {
		s.logsCache.Remove(id)
		s.chunksCache.Remove(id)
		s.lastChunkCache.Remove(id)
	}
	return dr, nil
}

// GetLastChunk implements logfs.LogsMetaStorage
func (s *CachedStorage) GetLastChunk(ctx context.Context, logID string) (logfs.ChunkInfo, error) {
	lci, err := s.lastChunkCache.GetOrCreate(logID)
	if err != nil {
		return logfs.ChunkInfo{}, err
	}
	return lci, nil
}

// GetChunks implements logfs.LogsMetaStorage
func (s *CachedStorage) GetChunks(ctx context.Context, logID string) ([]logfs.ChunkInfo, error) {
	return s.chunksCache.GetOrCreate(logID)
}

// UpsertChunkInfos implements logfs.LogsMetaStorage
func (s *CachedStorage) UpsertChunkInfos(ctx context.Context, logID string, cis []logfs.ChunkInfo) error {
	if err := s.storage.UpsertChunkInfos(ctx, logID, cis); err != nil {
		return err
	}
	s.chunksCache.Remove(logID)
	return nil
}
