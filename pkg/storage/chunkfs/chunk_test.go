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
package chunkfs

import (
	"crypto/rand"
	"github.com/oklog/ulid/v2"
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	"github.com/solarisdb/solaris/golibs/cast"
	"github.com/solarisdb/solaris/golibs/container"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/files"
	"github.com/solarisdb/solaris/golibs/ulidutils"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestMetaBuf_PutGet(t *testing.T) {
	mb := make(metaBuf, cMetaRecordSize*2)
	mr1 := metaRec{ID: ulidutils.New(), size: 1234, offset: 4356}
	mr2 := metaRec{ID: ulidutils.New(), size: 234, offset: 4334556}
	mb.put(0, mr1)
	mb.put(1, mr2)
	assert.Equal(t, mr1, mb.get(0))
	assert.Equal(t, mr2, mb.get(1))
	assert.Panics(t, func() {
		mb.get(2)
	})
	assert.Panics(t, func() {
		mb.put(2, mr2)
	})
}

func TestChunk_Open(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestChunk_Open")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	cfg := Config{NewSize: files.BlockSize, MaxChunkSize: 10 * files.BlockSize, MaxGrowIncreaseSize: 2 * files.BlockSize}

	fn := filepath.Join(dir, "c1")
	files.EnsureFileExists(fn)
	c := NewChunk(fn, "c1", cfg)
	_, err = c.AppendRecords(generateRecords(1, 1))
	assert.NotNil(t, err)
	_, err = c.OpenChunkReader(false)
	assert.NotNil(t, err)
	assert.Nil(t, c.Open(true))
	fi, err := os.Stat(fn)
	assert.Nil(t, err)
	assert.Equal(t, cfg.NewSize, fi.Size())
	assert.Nil(t, c.Close())
	assert.Nil(t, c.Open(false))

	// corrupting offsets
	buf, err := c.mmf.Buffer(8, 8)
	assert.Nil(t, err)
	copy(buf, hdrVersion)
	assert.Nil(t, c.Close())
	assert.NotNil(t, c.Open(false))
}

func TestChunk_SimpleAppend(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestChunk_SimpleAppend")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	cfg := Config{NewSize: files.BlockSize, MaxChunkSize: 10 * files.BlockSize, MaxGrowIncreaseSize: 2 * files.BlockSize}

	fn := filepath.Join(dir, "c1")
	files.EnsureFileExists(fn)
	c := NewChunk(fn, "c1", cfg)
	assert.Nil(t, c.Open(false))
	recs := generateRecords(3, 10)
	arr, err := c.AppendRecords(recs)
	assert.Nil(t, err)
	assert.Equal(t, 3, arr.Written)
	defer c.Close()

	cr1, err := c.OpenChunkReader(false)
	assert.Nil(t, err)
	defer cr1.Close()
	checkRecords(t, cr1, recs)

	var id ulid.ULID
	id.UnmarshalText(cast.StringToByteArray(recs[1].ID))
	cr1.SetStartID(id)
	checkRecords(t, cr1, recs[1:])
	cr1.SetStartID(ulidutils.New())
	checkRecords(t, cr1, nil)

	cr2, err := c.OpenChunkReader(true)
	assert.Nil(t, err)
	defer cr2.Close()

	container.SliceReverse(recs)
	checkRecords(t, cr2, recs)
	cr2.SetStartID(id)
	checkRecords(t, cr2, recs[1:])
	cr2.SetStartID(ulidutils.New())
	// all less!!!
	checkRecords(t, cr2, recs)
}

func TestChunk_AppendGrowth(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestChunk_AppendGrowth")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	cfg := Config{NewSize: files.BlockSize, MaxChunkSize: 10 * files.BlockSize, MaxGrowIncreaseSize: files.BlockSize}

	fn := filepath.Join(dir, "c1")
	files.EnsureFileExists(fn)
	c := NewChunk(fn, "c1", cfg)
	assert.Nil(t, c.Open(false))
	recs := generateRecords(3, 10)
	arr, err := c.AppendRecords(recs)
	assert.Nil(t, err)
	assert.Equal(t, 3, arr.Written)
	defer c.Close()

	fi, err := os.Stat(fn)
	assert.Nil(t, err)
	assert.Equal(t, cfg.NewSize, fi.Size())

	recs2 := generateRecords(100, 30)
	recs = append(recs, recs2...)
	_, err = c.AppendRecords(recs2)
	assert.Nil(t, err)
	fi, err = os.Stat(fn)
	assert.Nil(t, err)
	assert.Equal(t, 2*cfg.NewSize, fi.Size())

	_, err = c.AppendRecords(recs2)
	assert.Nil(t, err)
	fi, err = os.Stat(fn)
	assert.Nil(t, err)
	assert.Equal(t, 3*cfg.NewSize, fi.Size())
	recs = append(recs, recs2...)

	cr1, err := c.OpenChunkReader(false)
	assert.Nil(t, err)
	checkRecords(t, cr1, recs)
	cr1.Close()

	container.SliceReverse(recs)
	cr1, err = c.OpenChunkReader(true)
	assert.Nil(t, err)
	checkRecords(t, cr1, recs)
	cr1.Close()

	container.SliceReverse(recs)
	_, err = c.AppendRecords(recs2)
	assert.Nil(t, err)
	fi, err = os.Stat(fn)
	assert.Nil(t, err)
	assert.Equal(t, 4*cfg.NewSize, fi.Size())
	recs = append(recs, recs2...)

	before := c.freeOffset
	assert.Equal(t, len(recs), int(c.total))
	_, err = c.AppendRecords(generateRecords(1000, 30))
	assert.NotNil(t, err)
	assert.True(t, errors.Is(err, errors.ErrExhausted))
	assert.Equal(t, before, c.freeOffset)
	assert.Equal(t, len(recs), int(c.total))

	cr1, err = c.OpenChunkReader(false)
	assert.Nil(t, err)
	checkRecords(t, cr1, recs)
	cr1.Close()
}

func TestChunk_AppendGrowth2(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestChunk_AppendGrowth2")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	cfg := Config{NewSize: files.BlockSize, MaxChunkSize: 5 * files.BlockSize, MaxGrowIncreaseSize: files.BlockSize}

	fn := filepath.Join(dir, "c1")
	files.EnsureFileExists(fn)
	c := NewChunk(fn, "c1", cfg)
	assert.Nil(t, c.Open(false))
	defer c.Close()
	recs := generateRecords(3000, 512)
	arr, err := c.AppendRecords(recs)
	assert.Nil(t, err)
	assert.Equal(t, 38, arr.Written)
	assert.True(t, arr.StartID.Compare(arr.LastID) < 0)
}

func checkRecords(t *testing.T, it *ChunkReader, recs []*solaris.Record) {
	for _, rec := range recs {
		assert.True(t, it.HasNext())
		r, ok := it.Next()
		assert.True(t, ok)
		assert.Equal(t, rec.Payload, r.UnsafePayload)
		rec.ID = r.ID.String()
	}
	assert.False(t, it.HasNext())
}

func generateRecords(count, size int) []*solaris.Record {
	res := make([]*solaris.Record, count)
	for i := range res {
		b := make([]byte, size)
		rand.Read(b)
		res[i] = &solaris.Record{Payload: b}
	}
	return res
}
