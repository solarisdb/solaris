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
	"github.com/tidwall/buntdb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"slices"
)

type (
	// Config specifies configuration for logs meta storage
	// based on BuntDB https://github.com/tidwall/buntdb
	Config struct {
		// DBFilePath specifies path to the DB file
		// if empty the in-mem version is used
		DBFilePath string
	}

	// LogStorage is the logs meta storage,
	// implements the storage.Logs interface
	LogStorage struct {
		cfg    *Config
		db     *buntdb.DB
		eval   *storage.LogCondEval
		logger logging.Logger
	}

	entry struct {
		*solaris.Log
		Deleted bool `json:"deleted"`
	}
)

// NewLogStorage creates new logs meta storage based on BuntDB
func NewLogStorage(cfg Config) *LogStorage {
	return &LogStorage{cfg: &cfg, eval: storage.NewLogCondEval(ql.LogsCondDialect)}
}

// Init implements linker.Initializer
func (s *LogStorage) Init(ctx context.Context) error {
	path := s.cfg.DBFilePath
	if len(path) == 0 {
		path = ":memory:"
	}

	s.logger = logging.NewLogger("buntdb.LogStorage")
	s.logger.Infof("Initializing with dbFilePath=%s", path)

	var err error
	s.db, err = buntdb.Open(path)
	if err != nil {
		return fmt.Errorf("buntdb.Open(%s) failed: %w", path, err)
	}
	return nil
}

// Shutdown implements linker.Shutdowner
func (s *LogStorage) Shutdown() {
	s.logger.Infof("Shutting down...")
	if s.db != nil {
		_ = s.db.Close()
	}
}

// CreateLog implements storage.Logs
func (s *LogStorage) CreateLog(ctx context.Context, log *solaris.Log) (*solaris.Log, error) {
	e := toEntry(log)
	e.ID = ulidutils.NewID()
	e.CreatedAt = timestamppb.Now()
	e.UpdatedAt = e.CreatedAt
	val := mustMarshal(e)

	tx := mustBeginTx(s.db, true)
	defer mustRollback(tx)

	if _, _, err := tx.Set(e.ID, val, nil); err != nil {
		return nil, fmt.Errorf("tx.Set(%s, %s) failed: %w", e.ID, val, err)
	}
	mustCommit(tx)
	return toLog(e), nil
}

// GetLogByID implements storage.Logs
func (s *LogStorage) GetLogByID(ctx context.Context, id string) (*solaris.Log, error) {
	if len(id) == 0 {
		return nil, fmt.Errorf("id must be specified: %w", errors.ErrInvalid)
	}

	tx := mustBeginTx(s.db, false)
	defer mustRollback(tx)

	e, err := s.getEntry(tx, id, true)
	return toLog(e), err
}

// UpdateLog implements storage.Logs
func (s *LogStorage) UpdateLog(ctx context.Context, log *solaris.Log) (*solaris.Log, error) {
	if len(log.ID) == 0 {
		return nil, fmt.Errorf("log id must be specified: %w", errors.ErrInvalid)
	}

	tx := mustBeginTx(s.db, true)
	defer mustRollback(tx)

	_, err := s.getEntry(tx, log.ID, true)
	if err != nil {
		return nil, err
	}

	e := toEntry(log)
	e.UpdatedAt = timestamppb.Now()

	val := mustMarshal(e)
	if _, replaced, err := tx.Set(e.ID, val, nil); err != nil || !replaced {
		return nil, fmt.Errorf("tx.Set(%s, %s) failed (replaced=%t): %w", e.ID, val, replaced, err)
	}

	mustCommit(tx)
	return toLog(e), nil
}

// QueryLogs implements storage.Logs
func (s *LogStorage) QueryLogs(ctx context.Context, qr storage.QueryLogsRequest) (*solaris.QueryLogsResult, error) {
	var (
		qRes = &solaris.QueryLogsResult{}
		err  error
	)

	if len(qr.IDs) > 0 {
		qRes, err = s.queryByIDs(ctx, qr)
	} else if len(qr.Condition) > 0 {
		qRes, err = s.queryByCondition(ctx, qr, true)
	}
	if err != nil {
		return nil, fmt.Errorf("querly logs error: %w", err)
	}
	return qRes, nil
}

// DeleteLogs implements storage.Logs
func (s *LogStorage) DeleteLogs(ctx context.Context, req storage.DeleteLogsRequest) (*solaris.CountResult, error) {
	var (
		dRes = &solaris.CountResult{}
		err  error
	)

	if len(req.IDs) > 0 {
		dRes, err = s.deleteByIDs(ctx, req)
	} else if len(req.Condition) > 0 {
		dRes, err = s.deleteByCondition(ctx, req)
	}
	if err != nil {
		return nil, fmt.Errorf("delete logs error: %w", err)
	}
	return dRes, nil
}

func (s *LogStorage) deleteByIDs(ctx context.Context, req storage.DeleteLogsRequest) (*solaris.CountResult, error) {
	tx := mustBeginTx(s.db, true)
	defer mustRollback(tx)

	var deleted int64
	logIDs := slices.Clone(req.IDs)

	for _, id := range logIDs {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("context error: %w", ctx.Err())
		}
		if req.MarkOnly {
			e, err := s.getEntry(tx, id, true)
			if err != nil && errors.Is(err, errors.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, err
			}
			e.Deleted = true
			e.UpdatedAt = timestamppb.Now()
			val := mustMarshal(e)
			if _, replaced, err := tx.Set(e.ID, val, nil); err != nil || !replaced {
				return nil, fmt.Errorf("(markOnly=%t) tx.Set(%s, %s) failed (replaced=%t): %w", req.MarkOnly, e.ID, val, replaced, err)
			}
		} else {
			_, err := tx.Delete(id)
			if err != nil && errors.Is(err, buntdb.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("(markOnly=%t) tx.Delete(%s) failed: %w", req.MarkOnly, id, err)
			}
		}
		deleted++
	}

	mustCommit(tx)
	return &solaris.CountResult{
		Total: deleted,
	}, nil
}

func (s *LogStorage) deleteByCondition(ctx context.Context, req storage.DeleteLogsRequest) (*solaris.CountResult, error) {
	var logIDs []string
	qRes, err := s.queryByCondition(ctx, storage.QueryLogsRequest{Condition: req.Condition, Limit: 1000}, req.MarkOnly)
	for err == nil && len(qRes.Logs) > 0 {
		for _, log := range qRes.Logs {
			logIDs = append(logIDs, log.ID)
		}
		qRes.Logs = nil
		if len(qRes.NextPageID) > 0 {
			qRes, err = s.queryByCondition(ctx, storage.QueryLogsRequest{Condition: req.Condition,
				Page: qRes.NextPageID, Limit: 1000}, req.MarkOnly)
		}
	}
	if err != nil {
		return nil, err
	}
	return s.deleteByIDs(ctx, storage.DeleteLogsRequest{IDs: logIDs, MarkOnly: req.MarkOnly})
}

func (s *LogStorage) queryByIDs(ctx context.Context, qr storage.QueryLogsRequest) (*solaris.QueryLogsResult, error) {
	limit := min(int(qr.Limit), 1000)
	if qr.Limit == 0 {
		limit = 50
	}

	logIDs := slices.Clone(qr.IDs)
	slices.Sort(logIDs)
	if startIdx, found := slices.BinarySearch(logIDs, qr.Page); found {
		logIDs = logIDs[startIdx:]
	}

	tx := mustBeginTx(s.db, false)
	defer mustRollback(tx)

	var total int64
	var qLogs []*solaris.Log

	for _, id := range logIDs {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("context error: %w", ctx.Err())
		}
		if id < qr.Page {
			continue
		}
		e, err := s.getEntry(tx, id, true)
		if err != nil && errors.Is(err, errors.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		total++
		if len(qLogs) <= limit { // = for pagination
			qLogs = append(qLogs, e.Log)
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

func (s *LogStorage) queryByCondition(ctx context.Context, qr storage.QueryLogsRequest, skipMarkedDeleted bool) (*solaris.QueryLogsResult, error) {
	expr, err := ql.Parse(qr.Condition)
	if err != nil {
		return nil, fmt.Errorf("condition=%q parse error=%v: %w", qr.Condition, err, errors.ErrInvalid)
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
		e := mustUnmarshal(val)
		if skipMarkedDeleted && e.Deleted {
			return true
		}
		ok, evalErr := s.eval.Eval(e.Log, expr)
		if evalErr != nil {
			iterErr = fmt.Errorf("condition=%q eval error: %w", qr.Condition, evalErr)
			return false
		}
		if ok {
			total++
			if len(qLogs) <= limit { // = for pagination
				qLogs = append(qLogs, e.Log)
			}
		}
		return true
	}

	tx := mustBeginTx(s.db, false)
	defer mustRollback(tx)

	if err = tx.AscendGreaterOrEqual("", qr.Page, iter); err != nil {
		return nil, fmt.Errorf("quering failed: %w", err)
	}
	if iterErr != nil {
		return nil, err
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

func (s *LogStorage) getEntry(tx *buntdb.Tx, key string, skipMarkedDeleted bool) (*entry, error) {
	val, err := tx.Get(key, true)
	if err != nil && errors.Is(err, buntdb.ErrNotFound) {
		return nil, fmt.Errorf("entry does not exist: %w", errors.ErrNotExist)
	}
	if err != nil {
		return nil, fmt.Errorf("tx.Get(%s) failed: %w", key, err)
	}
	var e *entry
	if e = mustUnmarshal(val); skipMarkedDeleted && e.Deleted {
		return nil, errors.ErrNotExist
	}
	return e, nil
}

func mustMarshal(e *entry) string {
	if e == nil {
		return ""
	}
	bytes, err := json.Marshal(e)
	if err != nil {
		panic(fmt.Errorf("mustMarshal() failed: %v", err))
	}
	return cast.ByteArrayToString(bytes)
}

func mustUnmarshal(val string) *entry {
	bytes := cast.StringToByteArray(val)
	e := new(entry)
	if err := json.Unmarshal(bytes, e); err != nil {
		panic(fmt.Errorf("mustUnmarshal() failed: %v", err))
	}
	return e
}

func toEntry(log *solaris.Log) *entry {
	if log == nil {
		return nil
	}
	e := new(entry)
	e.Log = log
	return e
}

func toLog(e *entry) *solaris.Log {
	if e == nil {
		return nil
	}
	return e.Log
}
