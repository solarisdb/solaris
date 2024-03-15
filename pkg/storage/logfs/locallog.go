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

package logfs

import (
	"context"
	"fmt"
	"github.com/oklog/ulid/v2"
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	"github.com/solarisdb/solaris/golibs/cast"
	"github.com/solarisdb/solaris/golibs/container/lru"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/logging"
	"github.com/solarisdb/solaris/golibs/ulidutils"
	"github.com/solarisdb/solaris/pkg/storage"
	"github.com/solarisdb/solaris/pkg/storage/chunkfs"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sort"
	"sync"
)

type (
	// localLog implements Log interface for working with data stored in the chunks on the local file-system
	localLog struct {
		LMStorage    LogsMetaStorage   `inject:""`
		ChnkProvider *chunkfs.Provider `inject:""`

		logger  logging.Logger
		lockers *lru.ReleasableCache[string, *logLocker]
	}

	logLocker struct {
		lock sync.Mutex
	}

	// LogsMetaStorage interface describes a log meata storage for the log chunks info
	LogsMetaStorage interface {
		// GetLastChunk returns the chunk with the biggest chunkID
		GetLastChunk(ctx context.Context, logID string) (ChunkInfo, error)
		// GetChunks returns the list of chunks associated with the logID
		GetChunks(ctx context.Context, logID string) ([]ChunkInfo, error)
		// UpsertChunkInfos update or insert new records associated with logID into the meta-storage
		UpsertChunkInfos(ctx context.Context, logID string, cis []ChunkInfo) error
	}

	// ChunkInfo is the descriptor which describes a chunk information in the log meta-storage
	ChunkInfo struct {
		// ID is the chunk ID
		ID string
		// Min is the minimum (first) record ID stored in the chunk
		Min ulid.ULID
		// Max is the maximum (last) record ID stored in the chunk
		Max ulid.ULID
		// RecordsCount is the number of records stored in the chunk
		RecordsCount int
	}
)

var _ storage.Log = (*localLog)(nil)

const (
	// ChunkMinID defines the lower boundary for chunk ID (exclusive)
	ChunkMinID = ""
	// ChunkMaxID defines the upper boundary for chunk ID (exclusive)
	ChunkMaxID = "~"
)

// AppendRecords allows to write reocrds into the chunks on the local FS and update the Logs catalog with the new
// chunks created
func (l *localLog) AppendRecords(ctx context.Context, request *solaris.AppendRecordsRequest) (*solaris.AppendRecordsResult, error) {
	lid := request.LogID
	ll, err := l.lockers.GetOrCreate(ctx, lid)
	if err != nil {
		return nil, fmt.Errorf("could not obtain the log locker for id=%s: %w", lid, err)
	}
	defer l.lockers.Release(&ll)
	ll.Value().lock.Lock()
	defer ll.Value().lock.Unlock()

	cis := []ChunkInfo{}

	ci, err := l.LMStorage.GetLastChunk(ctx, lid)
	if err != nil && !errors.Is(err, errors.ErrNotExist) {
		return nil, err
	}

	recs := request.Records
	added := 0
	var gerr error
	for len(recs) > 0 {
		if ci.RecordsCount == 0 {
			ci = ChunkInfo{ID: ulidutils.NewID()}
			l.logger.Infof("creating new chunk id=%s for the logID=%s", ci.ID, lid)
		}
		rc, err := l.ChnkProvider.GetOpenedChunk(ctx, ci.ID)
		if err != nil {
			gerr = err
			break
		}
		arr, err := rc.Value().AppendRecords(recs)
		l.ChnkProvider.ReleaseChunk(&rc) // release the chunk ASAP
		if err != nil {
			gerr = err
			break
		}
		if arr.Written > 0 {
			if ci.RecordsCount == 0 {
				ci.Min = arr.StartID
			}
			ci.Max = arr.LastID
			ci.RecordsCount += arr.Written
			cis = append(cis, ci)
			recs = recs[arr.Written:]
			added += arr.Written
		} else if ci.RecordsCount == 0 {
			// the chunk was just created and its capacity is not enough to write at least one record!
			gerr = fmt.Errorf("It seems the maximum chunk size is less than the record size payload=%d: %w", len(recs[0].Payload), errors.ErrInvalid)
			break
		}
		ci.RecordsCount = 0
	}

	if added > 0 {
		// use context.Background instead of ctx to avoid some unrecoverable error in case of the ctx is closed, but we have some
		// data written
		if err := l.LMStorage.UpsertChunkInfos(context.Background(), lid, cis); err != nil {
			// well, now it is unrecoverable!
			l.logger.Errorf("could not write chunk IDs=%v for logID=%s, but the data is written into chunk. The data is corrupted now: %v", cis, lid, err)
			panic("unrecoverable error, data is corrupted")
		}
		if gerr != nil {
			l.logger.Warnf("AppendRecords: got the error=%v, but would be able to write some data for logID=%s, added=%d", gerr, lid, added)
		}
		gerr = nil // disregard the error, cause we could write something
	}

	return &solaris.AppendRecordsResult{Added: int64(added)}, gerr
}

func (l *localLog) QueryRecords(ctx context.Context, request storage.QueryRecordsRequest) ([]*solaris.Record, error) {
	lid := request.LogID

	// the l.lockers plays a role of limiter as well, it doesn't allow to have more than N locks available,
	// so the l.lockers.GetOrCreate(ctx, lid) will be blocked if number of requested locks (not the number of requests!)
	// exceeds the maximum (N) capacity.
	// We will request the lock for supporting the limited number of logs in a work a time, but will not to Lock it for
	// the read operation. Only AppendRecords does this to support its atomicy.
	ll, err := l.lockers.GetOrCreate(ctx, lid)
	if err != nil {
		return nil, fmt.Errorf("could not obtain the log locker for id=%s: %w", lid, err)
	}
	defer l.lockers.Release(&ll)

	cis, err := l.LMStorage.GetChunks(ctx, lid)
	if err != nil {
		return nil, err
	}

	if len(cis) == 0 {
		return nil, nil
	}

	var idx int
	inc := 1
	if request.Descending {
		inc = -1
		idx = len(cis) - 1
	}
	var sid ulid.ULID
	var empty ulid.ULID
	if request.StartID != "" {
		if err := sid.UnmarshalText(cast.StringToByteArray(request.StartID)); err != nil {
			l.logger.Warnf("could not unmarshal startID=%s: %v", request.StartID, err)
			return nil, fmt.Errorf("wrong startID=%q: %w", request.StartID, errors.ErrInvalid)
		}

		if request.Descending {
			idx = sort.Search(len(cis), func(i int) bool {
				return cis[i].Min.Compare(sid) > 0
			})
			idx--
			inc = -1
		} else {
			idx = sort.Search(len(cis), func(i int) bool {
				return cis[i].Max.Compare(sid) >= 0
			})
		}
	}

	limit := int(request.Limit)
	if limit > 10000 {
		limit = 10000
	}

	res := []*solaris.Record{}
	for idx >= 0 && idx < len(cis) && limit > len(res) {
		ci := cis[idx]
		srecs, err := l.readRecords(ctx, lid, ci, request.Descending, sid, limit-len(res))
		if err != nil {
			return nil, err
		}
		res = append(res, srecs...)
		idx += inc
		sid = empty
	}
	return res, nil
}

func (l *localLog) readRecords(ctx context.Context, lid string, ci ChunkInfo, descending bool, sid ulid.ULID, limit int) ([]*solaris.Record, error) {
	rc, err := l.ChnkProvider.GetOpenedChunk(ctx, ci.ID)
	if err != nil {
		return nil, err
	}
	defer l.ChnkProvider.ReleaseChunk(&rc)

	cr, err := rc.Value().OpenChunkReader(descending)
	if err != nil {
		return nil, err
	}
	defer cr.Close()

	var empty ulid.ULID
	if sid.Compare(empty) != 0 {
		cr.SetStartID(sid)
	}

	res := []*solaris.Record{}
	for cr.HasNext() && len(res) < limit {
		ur, _ := cr.Next()
		r := new(solaris.Record)
		r.ID = ur.ID.String()
		r.LogID = lid
		r.Payload = make([]byte, len(ur.UnsafePayload))
		copy(r.Payload, ur.UnsafePayload)
		r.CreatedAt = timestamppb.New(ulid.Time(ur.ID.Time()))
		res = append(res, r)
	}

	return res, nil
}
