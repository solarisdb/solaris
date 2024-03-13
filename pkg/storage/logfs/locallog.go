package logfs

import (
	"context"
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/logging"
	"github.com/solarisdb/solaris/golibs/ulidutils"
	"github.com/solarisdb/solaris/pkg/storage"
	"github.com/solarisdb/solaris/pkg/storage/chunkfs"
)

type (
	// localLog implements Log interface for working with data stored in the chunks on the local file-system
	localLog struct {
		logger    logging.Logger
		logs      storage.Logs
		cProvider *chunkfs.Provider
	}

	logLocker struct {
	}
)

var _ storage.Log = (*localLog)(nil)

func (l *localLog) AppendRecords(ctx context.Context, request *solaris.AppendRecordsRequest) (*solaris.AppendRecordsResult, error) {
	lid := request.LogID
	ci, err := l.logs.GetLastChunk(ctx, lid)
	if errors.Is(err, errors.ErrNotExist) {
		l.logger.Infof("AppendRecords(): there is no logID=%s, creating the first chunk then", lid)
		ci = storage.ChunkInfo{ID: ulidutils.NewID()}
		err = nil
	}
	if err != nil {
		return nil, err
	}
	_ = ci
	return nil, nil
}

func (l *localLog) QueryRecords(ctx context.Context, request storage.QueryRecordsRequest) ([]*solaris.Record, error) {
	//TODO implement me
	panic("implement me")
}
