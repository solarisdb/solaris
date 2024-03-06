package storage

import (
	"context"
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
)

type (
	// Logs provides an interface to manage the logs meta-information
	Logs interface {
		// CreateLog creates a new log and returns its descriptor with the new ID
		CreateLog(ctx context.Context, log *solaris.Log) (*solaris.Log, error)
		// GetLogByID returns Log by its ID. It returns the errors.ErrNotExist if the log is marked for delete or
		// or it doesn't exist
		GetLogByID(ctx context.Context, id string) (*solaris.Log, error)
		// UpdateLog update the Log object information. The Log is matched by the log ID
		UpdateLog(ctx context.Context, log *solaris.Log) (*solaris.Log, error)
		// QueryLogs returns the list of Log objects matched to the query request
		QueryLogs(ctx context.Context, qr QueryLogsReqeust) (*solaris.QueryLogsResult, error)
		// DeleteLogs allows to either mark or delete logs permanently
		DeleteLogs(ctx context.Context, request DeleteLogsRequest) (*solaris.DeleteLogsResult, error)
	}

	// QueryLogsReqeust is used for selecting list of known logs
	QueryLogsReqeust struct {
		Condition string
		// IDs is the list of Log IDs should be selected. If the value is not empty, the Condition field is disregarded
		IDs []string
		// Deleted search between deleted
		Deleted bool
		Page    string
		Limit   int64
	}

	// DeleteLogsRequest specifies the DeleteLogs parameters
	DeleteLogsRequest struct {
		Condition string
		// IDs is the list of Log IDs should be selected. If the value is not empty, the Condition field is disregarded
		IDs []string
		// MarkOnly allows not to delete the records physically, but mark it for deletion
		MarkOnly bool
	}

	// Log interface exposes an API for working with a Log records.
	Log interface {
		// AppendRecords allows to insert records into the log by its ID
		AppendRecords(ctx context.Context, request *solaris.AppendRecordsRequest) (*solaris.AppendRecordsResult, error)
	}
)
