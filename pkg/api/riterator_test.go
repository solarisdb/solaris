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
