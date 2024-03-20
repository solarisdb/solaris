package buntdb

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	"github.com/solarisdb/solaris/golibs/cast"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/logging"
	"github.com/solarisdb/solaris/golibs/ulidutils"
	"github.com/solarisdb/solaris/pkg/ql"
	"github.com/solarisdb/solaris/pkg/storage"
	"github.com/solarisdb/solaris/pkg/storage/logfs"
	"github.com/tidwall/buntdb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"slices"
	"strings"
)

type (
	// Config specifies configuration for logs meta storage
	// based on BuntDB https://github.com/tidwall/buntdb
	Config struct {
		// DBFilePath specifies path to the DB file
		// if empty the in-mem version is used
		DBFilePath string
	}

	// Storage is the logs meta storage
	Storage struct {
		cfg    *Config
		db     *buntdb.DB
		logger logging.Logger
	}

	logEntry struct {
		*solaris.Log
		Deleted bool `json:"deleted"`
	}

	chnkEntry struct {
		logfs.ChunkInfo
	}
)

// NewStorage creates new logs meta storage based on BuntDB
func NewStorage(cfg Config) *Storage {
	return &Storage{cfg: &cfg}
}

// Init implements linker.Initializer
func (s *Storage) Init(ctx context.Context) error {
	path := s.cfg.DBFilePath
	if len(path) == 0 {
		path = ":memory:"
	}

	s.logger = logging.NewLogger("buntdb.Storage")
	s.logger.Infof("Initializing with dbFilePath=%s", path)

	var err error
	s.db, err = buntdb.Open(path)
	if err != nil {
		return fmt.Errorf("buntdb.Open(%s) failed: %w", path, err)
	}
	return nil
}

// Shutdown implements linker.Shutdowner
func (s *Storage) Shutdown() {
	s.logger.Infof("Shutting down...")
	if s.db != nil {
		_ = s.db.Close()
	}
}

// ===================================== logs =====================================

// CreateLog implements storage.Logs
func (s *Storage) CreateLog(ctx context.Context, log *solaris.Log) (*solaris.Log, error) {
	le := toEntry(log)
	le.ID = ulidutils.NewID()
	le.CreatedAt = timestamppb.Now()
	le.UpdatedAt = le.CreatedAt

	tx := mustBeginTx(s.db, true)
	defer mustRollback(tx)

	key := logKey(le.ID)
	val := mustMarshal(le)

	if _, _, err := tx.Set(key, val, nil); err != nil {
		return nil, fmt.Errorf("tx.Set(%s, %s) failed: %w", key, val, err)
	}

	mustCommit(tx)
	return toLog(le), nil
}

// GetLogByID implements storage.Logs
func (s *Storage) GetLogByID(ctx context.Context, id string) (*solaris.Log, error) {
	if len(id) == 0 {
		return nil, fmt.Errorf("id must be specified: %w", errors.ErrInvalid)
	}

	tx := mustBeginTx(s.db, false)
	defer mustRollback(tx)

	e, err := s.getLogEntry(tx, logKey(id), true)
	return toLog(e), err
}

// UpdateLog implements storage.Logs
func (s *Storage) UpdateLog(ctx context.Context, log *solaris.Log) (*solaris.Log, error) {
	if len(log.ID) == 0 {
		return nil, fmt.Errorf("log id must be specified: %w", errors.ErrInvalid)
	}

	tx := mustBeginTx(s.db, true)
	defer mustRollback(tx)

	_, err := s.getLogEntry(tx, logKey(log.ID), true)
	if err != nil {
		return nil, err
	}

	le := toEntry(log)
	le.UpdatedAt = timestamppb.Now()

	key := logKey(le.ID)
	val := mustMarshal(le)

	var replaced bool
	if _, replaced, err = tx.Set(key, val, nil); err != nil || !replaced {
		return nil, fmt.Errorf("tx.Set(key=%s, val=%s) failed, replaced=%t: %w", key, val, replaced, err)
	}

	mustCommit(tx)
	return toLog(le), nil
}

// QueryLogs implements storage.Logs
func (s *Storage) QueryLogs(ctx context.Context, qr storage.QueryLogsRequest) (*solaris.QueryLogsResult, error) {
	var (
		qRes = &solaris.QueryLogsResult{}
		err  error
	)

	if len(qr.IDs) > 0 {
		qRes, err = s.queryLogsByIDs(ctx, qr, !qr.Deleted)
		if err != nil {
			return nil, fmt.Errorf("queryLogsByIDs(IDs=%v) failed: %w", qr.IDs, err)
		}
	} else if len(qr.Condition) > 0 {
		qRes, err = s.queryLogsByCondition(ctx, qr, !qr.Deleted)
		if err != nil {
			return nil, fmt.Errorf("queryLogsByCondition(Cond=%s) failed: %w", qr.Condition, err)
		}
	}
	return qRes, nil
}

// DeleteLogs implements storage.Logs
func (s *Storage) DeleteLogs(ctx context.Context, req storage.DeleteLogsRequest) (*solaris.CountResult, error) {
	var (
		dRes = &solaris.CountResult{}
		err  error
	)

	if len(req.IDs) > 0 {
		dRes, err = s.deleteLogsByIDs(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("deleteLogsByIDs(IDs=%v) failed: %w", req.IDs, err)
		}
	} else if len(req.Condition) > 0 {
		dRes, err = s.deleteLogsByCondition(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("deleteLogsByCondition(Cond=%s) failed: %w", req.Condition, err)
		}
	}
	return dRes, nil
}

func (s *Storage) deleteLogsByIDs(ctx context.Context, req storage.DeleteLogsRequest) (*solaris.CountResult, error) {
	tx := mustBeginTx(s.db, true)
	defer mustRollback(tx)

	var deleted int64
	for _, id := range slices.Clone(req.IDs) {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("context error: %w", ctx.Err())
		}
		if req.MarkOnly {
			if err := s.markLogDeleted(tx, id); err != nil {
				return nil, fmt.Errorf("markLogDeleted(ID=%s) failed: %w", id, err)
			}
		} else {
			if err := s.deleteLog(ctx, tx, id); err != nil && !errors.Is(err, errors.ErrNotExist) {
				return nil, fmt.Errorf("deleteLog(ID=%s) failed: %w", id, err)
			}
		}
		deleted++
	}

	mustCommit(tx)
	return &solaris.CountResult{
		Total: deleted,
	}, nil
}

func (s *Storage) deleteLog(ctx context.Context, tx *buntdb.Tx, logID string) error {
	key := logKey(logID)
	_, err := tx.Delete(key)
	if err != nil && errors.Is(err, buntdb.ErrNotFound) {
		return errors.ErrNotExist
	}
	if err != nil {
		return fmt.Errorf("tx.Delete(key=%s) failed: %w", key, err)
	}
	cis, err := getLogChunks(ctx, tx, logID)
	if err != nil {
		return fmt.Errorf("getLogChunks(ID=%s) failed: %w", logID, err)
	}
	for _, ci := range cis {
		key = chnkKey(logID, ci.ID)
		if _, err = tx.Delete(key); err != nil && errors.Is(err, buntdb.ErrNotFound) {
			return fmt.Errorf("tx.Delete(key=%s) failed: %w", key, err)
		}
	}
	return nil
}

func (s *Storage) markLogDeleted(tx *buntdb.Tx, logID string) error {
	le, err := s.getLogEntry(tx, logKey(logID), true)
	if err != nil && errors.Is(err, errors.ErrNotExist) {
		return errors.ErrNotExist
	}
	if err != nil {
		return err
	}

	le.Deleted = true
	le.UpdatedAt = timestamppb.Now()

	key := logKey(le.ID)
	val := mustMarshal(le)

	var replaced bool
	if _, replaced, err = tx.Set(key, val, nil); err != nil || !replaced {
		return fmt.Errorf("tx.Set(key=%s, val=%s) failed, replaced=%t: %w", key, val, replaced, err)
	}
	return nil
}

func (s *Storage) deleteLogsByCondition(ctx context.Context, req storage.DeleteLogsRequest) (*solaris.CountResult, error) {
	var logIDs []string
	qRes, err := s.queryLogsByCondition(ctx, storage.QueryLogsRequest{Condition: req.Condition, Limit: 1000}, req.MarkOnly)
	for err == nil && len(qRes.Logs) > 0 {
		for _, log := range qRes.Logs {
			logIDs = append(logIDs, log.ID)
		}
		qRes.Logs = nil
		if len(qRes.NextPageID) > 0 {
			qRes, err = s.queryLogsByCondition(ctx, storage.QueryLogsRequest{Condition: req.Condition,
				Page: qRes.NextPageID, Limit: 1000}, req.MarkOnly)
		}
	}
	if err != nil {
		return nil, err
	}
	return s.deleteLogsByIDs(ctx, storage.DeleteLogsRequest{IDs: logIDs, MarkOnly: req.MarkOnly})
}

func (s *Storage) queryLogsByIDs(ctx context.Context, qr storage.QueryLogsRequest, skipMarkedDeleted bool) (*solaris.QueryLogsResult, error) {
	limit := min(int(qr.Limit), 1000)
	if qr.Limit == 0 {
		limit = 50
	}

	logIDs := slices.Clone(qr.IDs)
	slices.Sort(logIDs)

	startIdx, _ := slices.BinarySearch(logIDs, qr.Page)
	if startIdx == len(logIDs) {
		return &solaris.QueryLogsResult{
			Logs:       nil,
			NextPageID: "",
			Total:      int64(len(logIDs)),
		}, nil
	}

	tx := mustBeginTx(s.db, false)
	defer mustRollback(tx)

	var total int64
	var qLogs []*solaris.Log

	for _, id := range logIDs[startIdx:] {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("context error: %w", ctx.Err())
		}
		le, err := s.getLogEntry(tx, logKey(id), skipMarkedDeleted)
		if err != nil && errors.Is(err, errors.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		total++
		if len(qLogs) <= limit { // = for pagination
			qLogs = append(qLogs, le.Log)
		}
	}

	var nextPageID string
	if len(qLogs) > limit {
		nextPageID = qLogs[limit].ID
		qLogs = qLogs[:limit]
	}
	return &solaris.QueryLogsResult{
		Logs:       qLogs,
		NextPageID: nextPageID,
		Total:      total,
	}, nil
}

func (s *Storage) queryLogsByCondition(ctx context.Context, qr storage.QueryLogsRequest, skipMarkedDeleted bool) (*solaris.QueryLogsResult, error) {
	expr, err := ql.Parse(qr.Condition)
	if err != nil {
		return nil, fmt.Errorf("condition=%q parse error=%v: %w", qr.Condition, err, errors.ErrInvalid)
	}
	tstF, err := ql.BuildExprF(expr, ql.LogsCondDialect)
	if err != nil {
		return nil, fmt.Errorf("could not compile condition=%s: %w", qr.Condition, err)
	}

	limit := min(int(qr.Limit), 1000)
	if qr.Limit == 0 {
		limit = 50
	}

	var total int64
	var iterErr error

	var qLogs []*solaris.Log
	iter := func(key, val string) bool {
		if ctx.Err() != nil {
			iterErr = fmt.Errorf("context error: %w", ctx.Err())
			return false
		}
		le := mustUnmarshal[logEntry](val)
		if skipMarkedDeleted && le.Deleted {
			return true
		}
		if tstF(le.Log) {
			total++
			if len(qLogs) <= limit { // = for pagination
				qLogs = append(qLogs, le.Log)
			}
		}
		return true
	}

	tx := mustBeginTx(s.db, false)
	defer mustRollback(tx)

	if err = tx.AscendGreaterOrEqual("", logKey(qr.Page), iter); err != nil {
		return nil, fmt.Errorf("iteration failed: %w", err)
	}
	if iterErr != nil {
		return nil, iterErr
	}

	var nextPageID string
	if len(qLogs) > limit {
		nextPageID = qLogs[limit].ID
		qLogs = qLogs[:limit]
	}
	return &solaris.QueryLogsResult{
		Logs:       qLogs,
		NextPageID: nextPageID,
		Total:      total,
	}, nil
}

func (s *Storage) getLogEntry(tx *buntdb.Tx, key string, skipMarkedDeleted bool) (logEntry, error) {
	val, err := getValue(tx, key)
	if err != nil {
		return logEntry{}, err
	}
	var le logEntry
	if le = mustUnmarshal[logEntry](val); skipMarkedDeleted && le.Deleted {
		return logEntry{}, errors.ErrNotExist
	}
	return le, nil
}

func logKey(id string) string {
	return fmt.Sprintf("/logs/%s", id)
}

// ===================================== chunks =====================================

// GetLastChunk implements logfs.LogsMetaStorage
func (s *Storage) GetLastChunk(ctx context.Context, logID string) (logfs.ChunkInfo, error) {
	tx := mustBeginTx(s.db, false)
	defer mustRollback(tx)

	var ce *chnkEntry
	iter := func(key, value string) bool {
		ce = mustUnmarshal[*chnkEntry](value)
		return false
	}

	if err := tx.DescendRange("", chnkKey(logID, logfs.ChunkMaxID), chnkKey(logID, logfs.ChunkMinID), iter); err != nil {
		return logfs.ChunkInfo{}, fmt.Errorf("iteration failed: %w", err)
	}
	if ce == nil {
		return logfs.ChunkInfo{}, errors.ErrNotExist
	}

	return ce.ChunkInfo, nil
}

// GetChunks implements logfs.LogsMetaStorage
func (s *Storage) GetChunks(ctx context.Context, logID string) ([]logfs.ChunkInfo, error) {
	tx := mustBeginTx(s.db, false)
	defer mustRollback(tx)

	if _, err := s.getLogEntry(tx, logKey(logID), true); err != nil {
		return nil, fmt.Errorf("getLogEntry(ID=%s) failed: %w", logID, err)
	}

	return getLogChunks(ctx, tx, logID)
}

// UpsertChunkInfos implements logfs.LogsMetaStorage
func (s *Storage) UpsertChunkInfos(ctx context.Context, logID string, cis []logfs.ChunkInfo) error {
	tx := mustBeginTx(s.db, true)
	defer mustRollback(tx)

	if _, err := s.getLogEntry(tx, logKey(logID), true); err != nil {
		return fmt.Errorf("getLogEntry(ID=%s) failed: %w", logID, err)
	}

	for _, chnk := range cis {
		if ctx.Err() != nil {
			return fmt.Errorf("context error: %w", ctx.Err())
		}
		if strings.TrimSpace(chnk.ID) == "" {
			return fmt.Errorf("invalid chunk ID=%s: %w", chnk.ID, errors.ErrInvalid)
		}

		key := chnkKey(logID, chnk.ID)
		val := mustMarshal(chnkEntry{ChunkInfo: chnk})

		if _, _, err := tx.Set(key, val, nil); err != nil {
			return fmt.Errorf("tx.Set(key=%s, val=%s) failed: %w", key, val, err)
		}
	}

	mustCommit(tx)
	return nil
}

func getLogChunks(ctx context.Context, tx *buntdb.Tx, logID string) ([]logfs.ChunkInfo, error) {
	var iterErr error
	var cis []logfs.ChunkInfo
	iter := func(key, value string) bool {
		if ctx.Err() != nil {
			iterErr = fmt.Errorf("context error: %w", ctx.Err())
			return false
		}
		cis = append(cis, mustUnmarshal[chnkEntry](value).ChunkInfo)
		return true
	}
	if err := tx.AscendRange("", chnkKey(logID, logfs.ChunkMinID), chnkKey(logID, logfs.ChunkMaxID), iter); err != nil {
		return nil, fmt.Errorf("iteration failed: %w", err)
	}
	if iterErr != nil {
		return nil, iterErr
	}
	return cis, nil
}

func chnkKey(logID, chnkID string) string {
	return fmt.Sprintf("%s/chunks/%s", logKey(logID), chnkID)
}

// ===================================== helpers =====================================

func mustBeginTx(db *buntdb.DB, writable bool) *buntdb.Tx {
	tx, err := db.Begin(writable)
	if err != nil {
		panic(fmt.Errorf("mustBeginTx(%t) failed: %v", writable, err))
	}
	return tx
}

func mustCommit(tx *buntdb.Tx) {
	if err := tx.Commit(); err != nil {
		panic(fmt.Errorf("mustCommit() failed: %v", err))
	}
}

func mustRollback(tx *buntdb.Tx) {
	if err := tx.Rollback(); err != nil && !errors.Is(err, buntdb.ErrTxClosed) {
		panic(fmt.Errorf("mustRollback() failed: %v", err))
	}
}

func getValue(tx *buntdb.Tx, key string) (string, error) {
	val, err := tx.Get(key, true)
	if err != nil && errors.Is(err, buntdb.ErrNotFound) {
		return "", errors.ErrNotExist
	}
	if err != nil {
		return "", fmt.Errorf("getValue(key=%s) failed: %w", key, err)
	}
	return val, nil
}

func mustMarshal[T any](obj T) string {
	bytes, err := json.Marshal(obj)
	if err != nil {
		panic(fmt.Errorf("mustMarshal() failed: %v", err))
	}
	return cast.ByteArrayToString(bytes)
}

func mustUnmarshal[T any](val string) T {
	bytes := cast.StringToByteArray(val)
	e := new(T)
	if err := json.Unmarshal(bytes, e); err != nil {
		panic(fmt.Errorf("mustUnmarshal() failed: %v", err))
	}
	return *e
}

func toEntry(log *solaris.Log) logEntry {
	return logEntry{Log: log}
}

func toLog(le logEntry) *solaris.Log {
	return le.Log
}
