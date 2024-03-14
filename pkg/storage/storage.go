// Copyright 2024 The Solaris Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
		// GetLogByID returns Log by its ID. It returns the errors.ErrNotExist if the log is marked for delete,
		// or it doesn't exist
		GetLogByID(ctx context.Context, id string) (*solaris.Log, error)
		// UpdateLog update the Log object information. The Log is matched by the log ID
		UpdateLog(ctx context.Context, log *solaris.Log) (*solaris.Log, error)
		// QueryLogs returns the list of Log objects matched to the query request
		QueryLogs(ctx context.Context, qr QueryLogsRequest) (*solaris.QueryLogsResult, error)
		// DeleteLogs allows to either mark or delete logs permanently
		DeleteLogs(ctx context.Context, request DeleteLogsRequest) (*solaris.CountResult, error)
	}

	// QueryLogsRequest is used for selecting list of known logs
	QueryLogsRequest struct {
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
		// IDs is the list of Log IDs should be deleted.
		IDs []string
		// MarkOnly allows not to delete the records physically, but mark it for deletion
		MarkOnly bool
	}

	// Log interface exposes an API for working with a Log records.
	Log interface {
		// AppendRecords allows to insert records into the log by its ID
		AppendRecords(ctx context.Context, request *solaris.AppendRecordsRequest) (*solaris.AppendRecordsResult, error)
		// QueryRecords allows to retrieve records by the request
		QueryRecords(ctx context.Context, request QueryRecordsRequest) ([]*solaris.Record, error)
	}

	QueryRecordsRequest struct {
		// Condition defines the filtering constrains
		Condition string
		// LogID where records should be read
		LogID string
		// descending specifies that the result should be sorted in the descending order
		Descending bool
		// StartID provides the first record ID it can be read (inclusive)
		StartID string
		// limit contains the number of records to be returned
		Limit int64
	}
)
