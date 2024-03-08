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
	"github.com/solarisdb/solaris/golibs/ulidutils"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

type (
	LogHelper struct {
		m map[string][]*solaris.Record
	}
)

var _ Log = (*LogHelper)(nil)

func NewLogHelper() *LogHelper {
	return &LogHelper{m: make(map[string][]*solaris.Record)}
}

func (l *LogHelper) AppendRecords(ctx context.Context, request *solaris.AppendRecordsRequest) (*solaris.AppendRecordsResult, error) {
	recs, ok := l.m[request.LogID]
	if !ok {
		recs = []*solaris.Record{}
	}
	recs = append(recs, request.Records...)
	for i, r := range recs {
		r.LogID = request.LogID
		r.ID = ulidutils.NewID()
		r.CreatedAt = timestamppb.New(time.Now())
		recs[i] = r
	}
	l.m[request.LogID] = recs
	return &solaris.AppendRecordsResult{Added: int64(len(request.Records))}, nil
}

func (l *LogHelper) QueryRecords(ctx context.Context, request QueryRecordsRequest) ([]*solaris.Record, error) {
	res := []*solaris.Record{}
	recs := l.m[request.LogID]
	if request.Descending {
		idx := len(recs) - 1
		if request.StartID != "" {
			for idx >= 0 && recs[idx].ID > request.StartID {
				idx--
			}
		}
		for idx >= 0 && request.Limit > 0 {
			res = append(res, recs[idx])
			idx--
			request.Limit--
		}
	} else {
		idx := 0
		if request.StartID != "" {
			for idx < len(recs) && recs[idx].ID < request.StartID {
				idx++
			}
		}
		for idx < len(recs) && request.Limit > 0 {
			res = append(res, recs[idx])
			idx++
			request.Limit--
		}
	}
	return res, nil
}
