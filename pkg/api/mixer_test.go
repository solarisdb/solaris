package api

import (
	context2 "context"
	"fmt"
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	"github.com/solarisdb/solaris/golibs/cast"
	"github.com/solarisdb/solaris/golibs/container/iterable"
	"github.com/solarisdb/solaris/golibs/context"
	"github.com/solarisdb/solaris/pkg/storage"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMixer_NoLogs(t *testing.T) {
	mx := newMixer(context2.Background(), nil, nil, storage.QueryRecordsRequest{}, nil)
	assert.False(t, mx.HasNext())
	_, ok := mx.Next()
	assert.False(t, ok)
}

func TestMixer_OneIterator(t *testing.T) {
	recs := make([]*solaris.Record, 10)
	for i := 0; i < 10; i++ {
		recs[i] = &solaris.Record{Payload: []byte(fmt.Sprintf("m%d", i))}
	}
	ls := storage.NewLogHelper()
	ls.AppendRecords(context2.Background(), &solaris.AppendRecordsRequest{Records: recs, LogID: "1"})

	ctx, cancel := context.WithCancelError(context2.Background())
	baseQuery := storage.QueryRecordsRequest{Limit: 100}
	mx := newMixer(ctx, cancel, ls, baseQuery, []string{"1"})
	idx := 0
	for mx.HasNext() {
		r, ok := mx.Next()
		assert.True(t, ok)
		assert.Equal(t, recs[idx].Payload, r.Payload)
		recs[idx] = r
		idx++
	}

	baseQuery = storage.QueryRecordsRequest{LogID: "1", Limit: 1, StartID: recs[5].ID}
	mx = newMixer(ctx, cancel, ls, baseQuery, []string{"1"})
	idx = 5
	for mx.HasNext() {
		r, ok := mx.Next()
		assert.True(t, ok)
		assert.Equal(t, recs[idx], r)
		idx++
	}

	baseQuery = storage.QueryRecordsRequest{LogID: "1", Limit: 1, Descending: true, StartID: recs[5].ID}
	mx = newMixer(ctx, cancel, ls, baseQuery, []string{"1"})
	idx = 5
	for mx.HasNext() {
		r, ok := mx.Next()
		assert.True(t, ok)
		assert.Equal(t, recs[idx], r)
		idx--
	}
}

func TestMixer_ThreeIterators(t *testing.T) {
	recs := make([]*solaris.Record, 5)
	for i := 0; i < len(recs); i++ {
		recs[i] = &solaris.Record{Payload: []byte(fmt.Sprintf("%d", i))}
	}
	ls := storage.NewLogHelper()
	for i := 0; i < len(recs); i += 2 {
		end := i + 2
		if end > len(recs) {
			end = len(recs)
		}
		ls.AppendRecords(context2.Background(), &solaris.AppendRecordsRequest{Records: recs[i:end], LogID: fmt.Sprintf("%d", i/2)})
	}

	ctx, cancel := context.WithCancelError(context2.Background())
	baseQuery := storage.QueryRecordsRequest{Limit: 100}
	mx := newMixer(ctx, cancel, ls, baseQuery, []string{"0", "2", "1"})
	ids := testPayloads(t, mx, []string{"0", "1", "2", "3", "4"})

	baseQuery = storage.QueryRecordsRequest{StartID: ids[2], Limit: 100}
	mx = newMixer(ctx, cancel, ls, baseQuery, []string{"0", "2", "1"})
	_ = testPayloads(t, mx, []string{"2", "3", "4"})

	baseQuery = storage.QueryRecordsRequest{Descending: true, Limit: 100}
	mx = newMixer(ctx, cancel, ls, baseQuery, []string{"0", "2", "1"})
	testPayloads(t, mx, []string{"4", "3", "2", "1", "0"})

	baseQuery = storage.QueryRecordsRequest{Descending: true, StartID: ids[2], Limit: 100}
	mx = newMixer(ctx, cancel, ls, baseQuery, []string{"0", "2", "1"})
	_ = testPayloads(t, mx, []string{"2", "1", "0"})

	baseQuery = storage.QueryRecordsRequest{Limit: 100}
	mx = newMixer(ctx, cancel, ls, baseQuery, []string{"0", "1"})
	testPayloads(t, mx, []string{"0", "1", "2", "3"})

	baseQuery = storage.QueryRecordsRequest{Limit: 1}
	mx = newMixer(ctx, cancel, ls, baseQuery, []string{"0", "2"})
	testPayloads(t, mx, []string{"0", "1", "4"})
}

func testPayloads(t *testing.T, it iterable.Iterator[*solaris.Record], payloads []string) []string {
	ids := []string{}
	for _, p := range payloads {
		assert.True(t, it.HasNext())
		r, ok := it.Next()
		assert.True(t, ok)
		assert.Equal(t, r.Payload, cast.StringToByteArray(p))
		ids = append(ids, r.ID)
	}
	assert.False(t, it.HasNext())
	return ids
}
