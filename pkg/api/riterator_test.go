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

package api

import (
	context2 "context"
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	"github.com/solarisdb/solaris/golibs/context"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/ulidutils"
	"github.com/solarisdb/solaris/pkg/storage"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRIterator_Next(t *testing.T) {
	ctx, cancel := context.WithCancelError(context2.Background())

	ls := storage.NewLogHelper()
	baseQuery := storage.QueryRecordsRequest{LogID: "1", Limit: 1}
	rit := newRIterator(ctx, cancel, ls, baseQuery)
	assert.False(t, rit.HasNext())
	rit.Close()

	recs := []*solaris.Record{
		{Payload: []byte("m1")},
		{Payload: []byte("m2")},
		{Payload: []byte("m3")},
	}
	ls.AppendRecords(ctx, &solaris.AppendRecordsRequest{Records: recs, LogID: "1"})
	rit = newRIterator(ctx, cancel, ls, baseQuery)
	assert.True(t, rit.HasNext())

	for i, r := range recs {
		r1, ok := rit.Next()
		assert.True(t, ok)
		assert.Equal(t, r.Payload, r1.Payload)
		recs[i] = r1
	}

	assert.False(t, rit.HasNext())

	rit = newRIterator(ctx, cancel, ls, baseQuery)
	assert.True(t, rit.HasNext())
	cancel(errors.ErrInvalid)
	assert.False(t, rit.HasNext())

	ctx, cancel = context.WithCancelError(context2.Background())
	rit = newRIterator(ctx, cancel, ls, baseQuery)
	r, ok := rit.Next()
	assert.True(t, ok)
	assert.Equal(t, recs[0], r)
	cancel(errors.ErrInvalid)
	r, ok = rit.Next()
	assert.False(t, ok)
	assert.Nil(t, r)
}

func TestRIterator_Forward(t *testing.T) {
	ctx, cancel := context.WithCancelError(context2.Background())

	ls := storage.NewLogHelper()
	baseQuery := storage.QueryRecordsRequest{LogID: "1", Limit: 100}
	rit := newRIterator(ctx, cancel, ls, baseQuery)
	assert.False(t, rit.HasNext())
	rit.Close()

	recs := []*solaris.Record{
		{Payload: []byte("m1")},
		{Payload: []byte("m2")},
		{Payload: []byte("m3")},
	}
	ls.AppendRecords(ctx, &solaris.AppendRecordsRequest{Records: recs, LogID: "1"})
	rit = newRIterator(ctx, cancel, ls, baseQuery)
	assert.True(t, rit.HasNext())

	for i, r := range recs {
		r1, ok := rit.Next()
		assert.True(t, ok)
		assert.Equal(t, r.Payload, r1.Payload)
		recs[i] = r1
	}

	assert.False(t, rit.HasNext())

	baseQuery.Limit = 1
	rit = newRIterator(ctx, cancel, ls, baseQuery)

	for _, r := range recs {
		r1, ok := rit.Next()
		assert.True(t, ok)
		assert.Equal(t, r, r1)
	}

	baseQuery.StartID = recs[1].ID
	rit = newRIterator(ctx, cancel, ls, baseQuery)
	for i := 1; i < len(recs); i++ {
		r1, ok := rit.Next()
		assert.True(t, ok)
		assert.Equal(t, recs[i], r1)
	}

	baseQuery.StartID = ulidutils.NextID(recs[2].ID)
	rit = newRIterator(ctx, cancel, ls, baseQuery)
	assert.False(t, rit.HasNext())
}

func TestRIterator_Backward(t *testing.T) {
	ctx, cancel := context.WithCancelError(context2.Background())

	ls := storage.NewLogHelper()
	baseQuery := storage.QueryRecordsRequest{LogID: "1", Limit: 100}
	rit := newRIterator(ctx, cancel, ls, baseQuery)
	assert.False(t, rit.HasNext())
	rit.Close()

	recs := []*solaris.Record{
		{Payload: []byte("m1")},
		{Payload: []byte("m2")},
		{Payload: []byte("m3")},
	}
	ls.AppendRecords(ctx, &solaris.AppendRecordsRequest{Records: recs, LogID: "1"})
	rit = newRIterator(ctx, cancel, ls, baseQuery)
	assert.True(t, rit.HasNext())

	for i, r := range recs {
		r1, ok := rit.Next()
		assert.True(t, ok)
		assert.Equal(t, r.Payload, r1.Payload)
		recs[i] = r1
	}

	assert.False(t, rit.HasNext())

	baseQuery.Descending = true
	rit = newRIterator(ctx, cancel, ls, baseQuery)

	for i := len(recs) - 1; i >= 0; i-- {
		r1, ok := rit.Next()
		assert.True(t, ok)
		assert.Equal(t, recs[i], r1)
	}

	baseQuery.Limit = 1
	rit = newRIterator(ctx, cancel, ls, baseQuery)

	for i := len(recs) - 1; i >= 0; i-- {
		r1, ok := rit.Next()
		assert.True(t, ok)
		assert.Equal(t, recs[i], r1)
	}

	baseQuery.StartID = recs[1].ID
	rit = newRIterator(ctx, cancel, ls, baseQuery)

	for i := len(recs) - 2; i >= 0; i-- {
		r1, ok := rit.Next()
		assert.True(t, ok)
		assert.Equal(t, recs[i], r1)
	}

	baseQuery.StartID = ulidutils.PrevID(recs[0].ID)
	rit = newRIterator(ctx, cancel, ls, baseQuery)
	assert.False(t, rit.HasNext())
}
