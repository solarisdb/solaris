package api

import (
	"context"
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/logging"
	"github.com/solarisdb/solaris/pkg/storage"
)

// Service implements the grpc public API (see solaris.ServiceServer)
type Service struct {
	solaris.UnimplementedServiceServer
	logger logging.Logger

	LogsStorage storage.Logs `inject:""`
	LogStorage  storage.Log  `inject:""`
}

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
	res, err := s.LogsStorage.QueryLogs(ctx, storage.QueryLogsReqeust{Condition: request.Condition, Page: request.PageID, Limit: request.Limit})
	if err != nil {
		s.logger.Warnf("could not query=%v: %v", request, err)
	}
	return res, errors.GRPCWrap(err)
}

func (s *Service) DeleteLogs(ctx context.Context, request *solaris.DeleteLogsRequest) (*solaris.DeleteLogsResult, error) {
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
	//TODO implement me
	panic("implement me")
}
