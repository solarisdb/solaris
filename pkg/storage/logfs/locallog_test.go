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
	rand2 "crypto/rand"
	"fmt"
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	"github.com/solarisdb/solaris/golibs/container"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/files"
	"github.com/solarisdb/solaris/golibs/logging"
	"github.com/solarisdb/solaris/pkg/storage"
	"github.com/solarisdb/solaris/pkg/storage/chunkfs"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"sync"
	"testing"
)

func TestNewLocalLog(t *testing.T) {
	l := NewLocalLog(GetDefaultConfig())
	l.Shutdown()
	assert.Panics(t, func() {
		NewLocalLog(Config{})
	})
}

func TestReadWriteSimple(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestReadWriteSimple")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	p := chunkfs.NewProvider(dir, 1, chunkfs.GetDefaultConfig())
	defer p.Close()

	ll := NewLocalLog(GetDefaultConfig())
	ll.LMStorage = newTestLogsMetaStorage()
	ll.ChnkProvider = p
	defer ll.Shutdown()

	recs := generateRecords(100, 1024)
	res, err := ll.AppendRecords(context.Background(), &solaris.AppendRecordsRequest{Records: recs[:25], LogID: "l1"})
	assert.Nil(t, err)
	assert.Equal(t, 25, int(res.Added))
	res, err = ll.AppendRecords(context.Background(), &solaris.AppendRecordsRequest{Records: recs[25:], LogID: "l1"})
	assert.Nil(t, err)
	assert.Equal(t, 75, int(res.Added))

	qrecs, more, err := ll.QueryRecords(context.Background(), storage.QueryRecordsRequest{LogID: "l1", Limit: 100000})
	assert.Nil(t, err)
	assert.False(t, more)
	comparePayloads(t, qrecs, recs)

	startID := qrecs[50].ID
	qrecs, more, err = ll.QueryRecords(context.Background(), storage.QueryRecordsRequest{LogID: "l1", StartID: startID, Limit: 100000})
	assert.Nil(t, err)
	assert.False(t, more)
	comparePayloads(t, qrecs, recs[50:])

	container.SliceReverse(recs)
	qrecs, more, err = ll.QueryRecords(context.Background(), storage.QueryRecordsRequest{LogID: "l1", Descending: true, Limit: 100000})
	assert.Nil(t, err)
	assert.False(t, more)
	comparePayloads(t, qrecs, recs)

	qrecs, more, err = ll.QueryRecords(context.Background(), storage.QueryRecordsRequest{LogID: "l1", StartID: startID, Descending: true, Limit: 100000})
	assert.Nil(t, err)
	assert.False(t, more)
	assert.Equal(t, startID, qrecs[0].ID)
	comparePayloads(t, qrecs, recs[49:])
}

func TestAppendRecordsLimits(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestAppendRecordsLimits")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	p := chunkfs.NewProvider(dir, 1, chunkfs.Config{
		NewSize:             files.BlockSize,
		MaxChunkSize:        2 * files.BlockSize,
		MaxGrowIncreaseSize: files.BlockSize,
	})
	defer p.Close()

	ll := NewLocalLog(Config{
		MaxRecordsLimit: 5,
		MaxBunchSize:    5 * files.BlockSize,
		MaxLocks:        1,
	})
	ll.LMStorage = newTestLogsMetaStorage()
	ll.ChnkProvider = p
	defer ll.Shutdown()

	// will split onto two chunks
	recs := generateRecords(2, files.BlockSize)
	recs = append(recs, generateRecords(1, files.BlockSize*2)...) // this one will not fit
	res, err := ll.AppendRecords(context.Background(), &solaris.AppendRecordsRequest{Records: recs, LogID: "l1"})
	assert.Nil(t, err)
	assert.Equal(t, int64(2), res.Added)

	cis, err := ll.LMStorage.GetChunks(context.Background(), "l1")
	assert.Nil(t, err)
	assert.Equal(t, 2, len(cis))
	assert.Equal(t, 1, cis[0].RecordsCount)
	assert.Equal(t, 1, cis[1].RecordsCount)

	ll.Shutdown()
	_, err = ll.AppendRecords(context.Background(), &solaris.AppendRecordsRequest{Records: recs, LogID: "l1"})
	assert.True(t, errors.Is(err, errors.ErrClosed))
}

func TestQueryRecords(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestQueryRecords")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	p := chunkfs.NewProvider(dir, 1, chunkfs.Config{
		NewSize:             files.BlockSize,
		MaxChunkSize:        2 * files.BlockSize,
		MaxGrowIncreaseSize: files.BlockSize,
	})
	defer p.Close()

	ll := NewLocalLog(Config{
		MaxRecordsLimit: 10,
		MaxBunchSize:    files.BlockSize,
		MaxLocks:        1,
	})
	ll.LMStorage = newTestLogsMetaStorage()
	ll.ChnkProvider = p
	defer ll.Shutdown()

	recs1 := generateRecords(12, 100)
	res, err := ll.AppendRecords(context.Background(), &solaris.AppendRecordsRequest{Records: recs1, LogID: "l1"})
	assert.Nil(t, err)
	assert.Equal(t, int64(12), res.Added)

	recs2 := generateRecords(20, files.BlockSize/2)
	res, err = ll.AppendRecords(context.Background(), &solaris.AppendRecordsRequest{Records: recs2, LogID: "l1"})
	assert.Nil(t, err)
	assert.Equal(t, int64(20), res.Added)

	qrecs, more, err := ll.QueryRecords(context.Background(), storage.QueryRecordsRequest{LogID: "l1", Limit: 100000})
	assert.Nil(t, err)
	assert.True(t, more)
	assert.Equal(t, 10, len(qrecs))
	comparePayloads(t, qrecs, recs1[:10])

	qrecs, more, err = ll.QueryRecords(context.Background(), storage.QueryRecordsRequest{LogID: "l1", StartID: qrecs[9].ID, Limit: 100000})
	assert.Nil(t, err)
	assert.True(t, more)
	assert.Equal(t, 5, len(qrecs))

	qrecs, more, err = ll.QueryRecords(context.Background(), storage.QueryRecordsRequest{LogID: "l1", StartID: qrecs[4].ID, Limit: 100000})
	assert.Nil(t, err)
	assert.True(t, more)
	assert.Equal(t, 2, len(qrecs))

	ll.Shutdown()
	_, _, err = ll.QueryRecords(context.Background(), storage.QueryRecordsRequest{LogID: "l1", Limit: 100})
	assert.True(t, errors.Is(err, errors.ErrClosed))
}

func TestConcurrentMess(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestConcurrentMess2")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	p := chunkfs.NewProvider(dir, 1, chunkfs.Config{
		NewSize:             files.BlockSize,
		MaxChunkSize:        2 * files.BlockSize,
		MaxGrowIncreaseSize: files.BlockSize,
	})
	defer p.Close()

	ll := NewLocalLog(Config{
		MaxRecordsLimit: 1000,
		MaxBunchSize:    10 * files.BlockSize,
		MaxLocks:        1,
	})
	ll.LMStorage = newTestLogsMetaStorage()
	ll.ChnkProvider = p
	defer ll.Shutdown()

	m := map[string][]*solaris.Record{}
	for i := 0; i < 10; i++ {
		lid := fmt.Sprintf("%d", i)
		ln := rand.Intn(50) + 50
		recs := generateRecords(ln, 200)
		res, err := ll.AppendRecords(context.Background(), &solaris.AppendRecordsRequest{Records: recs, LogID: lid})
		assert.Nil(t, err)
		assert.Equal(t, int64(ln), res.Added)
		m[lid] = recs
	}

	log := logging.NewLogger("testLogger")
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			lid := fmt.Sprintf("%d", rand.Intn(10))
			log.Infof("reading from lid=%s, expecting %d records", lid, len(m[lid]))
			qrecs, more, err := ll.QueryRecords(context.Background(), storage.QueryRecordsRequest{LogID: lid, Limit: 1000})
			assert.Nil(t, err)
			assert.False(t, more)
			log.Infof("read %d records from the lid=%s", len(qrecs), lid)
			comparePayloads(t, qrecs, m[lid])
			wg.Done()
		}()
	}
	wg.Wait()
}

func comparePayloads(t *testing.T, a, b []*solaris.Record) {
	assert.Equal(t, len(a), len(b))
	for i, v := range a {
		assert.Equal(t, v.Payload, b[i].Payload)
	}
}

func generateRecords(count, size int) []*solaris.Record {
	res := make([]*solaris.Record, count)
	for i := range res {
		b := make([]byte, size)
		rand2.Read(b)
		res[i] = &solaris.Record{Payload: b}
	}
	return res
}
