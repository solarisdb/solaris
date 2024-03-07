package api

import (
	"context"
	"fmt"
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	context2 "github.com/solarisdb/solaris/golibs/context"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/logging"
	"github.com/solarisdb/solaris/pkg/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service implements the grpc public API (see solaris.ServiceServer)
type Service struct {
	solaris.UnimplementedServiceServer
	logger logging.Logger

	LogsStorage storage.Logs `inject:""`
	LogStorage  storage.Log  `inject:""`
}

const maxLogsToMerge = 1000

var _ solaris.ServiceServer = (*Service)(nil)

func NewService() *Service {
	return &Service{
		logger: logging.NewLogger("api.Service"),
	}
}

func (s *Service) CreateLog(ctx context.Context, log *solaris.Log) (*solaris.Log, error) {
	s.logger.Infof("create new log: %v", log)
	res, err := s.LogsStorage.CreateLog(ctx, log)
	if err != nil {
		s.logger.Warnf("could not create log=%v: %v", log, err)
	}
	return res, errors.GRPCWrap(err)
}

func (s *Service) UpdateLog(ctx context.Context, log *solaris.Log) (*solaris.Log, error) {
	s.logger.Infof("updating log: %v", log)
	res, err := s.LogsStorage.UpdateLog(ctx, log)
	if err != nil {
		s.logger.Warnf("could not update log=%v: %v", log, err)
	}
	return res, errors.GRPCWrap(err)
}

func (s *Service) QueryLogs(ctx context.Context, request *solaris.QueryLogsRequest) (*solaris.QueryLogsResult, error) {
	res, err := s.LogsStorage.QueryLogs(ctx, storage.QueryLogsRequest{Condition: request.Condition, Page: request.PageID, Limit: request.Limit})
	if err != nil {
		s.logger.Warnf("could not query=%v: %v", request, err)
	}
	return res, errors.GRPCWrap(err)
}

func (s *Service) DeleteLogs(ctx context.Context, request *solaris.DeleteLogsRequest) (*solaris.CountResult, error) {
	s.logger.Infof("delete logs: %v", request)
	res, err := s.LogsStorage.DeleteLogs(ctx, storage.DeleteLogsRequest{Condition: request.Condition, MarkOnly: true})
	if err != nil {
		s.logger.Warnf("could not delete logs for the request=%v: %v", err)
	} else {
		s.logger.Infof("%d records marked for delete for request=%v", res.Total, request)
	}
	return res, errors.GRPCWrap(err)
}

func (s *Service) AppendRecords(ctx context.Context, request *solaris.AppendRecordsRequest) (*solaris.AppendRecordsResult, error) {
	_, err := s.LogsStorage.GetLogByID(ctx, request.LogID)
	if err != nil {
		return nil, errors.GRPCWrap(err)
	}
	res, err := s.LogStorage.AppendRecords(ctx, request)
	if err != nil {
		s.logger.Warnf("could not append records to logID=%s: %v", request.LogID, err)
	}
	return res, errors.GRPCWrap(err)
}

func (s *Service) QueryRecords(ctx context.Context, request *solaris.QueryRecordsRequest) (*solaris.QueryRecordsResult, error) {
	logIDs := request.LogIDs
	if len(logIDs) == 0 {
		// requesting maxLogsToMerge+1 to be sure that if we have more than the maximum, will interrupt the procedure
		qr, err := s.LogsStorage.QueryLogs(ctx, storage.QueryLogsReqeust{Condition: request.LogsCondition, Limit: int64(maxLogsToMerge + 1)})
		if err != nil {
			return nil, errors.GRPCWrap(err)
		}
		logIDs := make([]string, len(qr.Logs))
		for i, l := range qr.Logs {
			logIDs[i] = l.ID
		}
	}
	if len(logIDs) > maxLogsToMerge {
		return nil, errors.GRPCWrap(fmt.Errorf("could not merge more than %d logs together: %w", maxLogsToMerge, errors.ErrExhausted))
	}

	if len(logIDs) == 1 {
		res, err := s.LogStorage.QueryRecords(ctx, storage.QueryRecordsRequest{Condition: request.Condition,
			LogID: logIDs[0], Descending: request.Descending, StartID: request.StartRecordID, Limit: request.Limit + 1})
		if err != nil {
			return nil, errors.GRPCWrap(err)
		}
		nextID := ""
		if int64(len(res)) > request.Limit {
			nextID = res[len(res)-1].ID
			res = res[:len(res)-1]
		}
		return &solaris.QueryRecordsResult{Records: res, NextPageID: nextID}, nil
	}

	ctx, cancel := context2.WithCancelError(ctx)
	defer cancel(nil)

	baseQuery := storage.QueryRecordsRequest{Condition: request.Condition,
		Descending: request.Descending, StartID: request.StartRecordID, Limit: request.Limit}
	mx := newMixer(ctx, cancel, s.LogStorage, baseQuery, logIDs)

	lim := request.Limit

	res := make([]*solaris.Record, 0, lim)
	for mx.HasNext() && lim > 0 {
		r, ok := mx.Next()
		if !ok {
			break
		}
		lim--
		res = append(res, r)
	}

	nextID := ""
	if mx.HasNext() {
		if r, ok := mx.Next(); ok {
			nextID = r.ID
		}
	}

	// while the iteration above we could get an error, so check it out
	err := ctx.Err()
	if err != nil {
		s.logger.Errorf("could not read data for the request=%v: %v", request, err)
	}
	return &solaris.QueryRecordsResult{Records: res, NextPageID: nextID}, errors.GRPCWrap(err)
}

func (s *Service) CountRecords(context.Context, *solaris.QueryRecordsRequest) (*solaris.CountResult, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CountRecords not implemented")
}
