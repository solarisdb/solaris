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
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/oklog/ulid/v2"
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	"github.com/solarisdb/solaris/golibs/container/iterable"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/files"
	"github.com/solarisdb/solaris/golibs/logging"
	"github.com/solarisdb/solaris/golibs/ulidutils"
	"sort"
	"sync"
)

type (
	// Chunk allows to write data into a file.
	Chunk struct {
		id   string
		fn   string
		cfg  Config
		mmf  *files.MMFile
		lock sync.RWMutex
		// freeOffset points to the first available byte for write
		freeOffset int
		// total contains number of records
		total  int
		logger logging.Logger
	}

	// ChunkReader is a helper structure which allows to read records from a chunk. The ChunkReader
	// implements interable.Iterator interface. When a ChunkReader is opened, the Write operations to the chunk are blocked,
	// so the records must be read ASAP and the ChunkReader must be closed.
	ChunkReader struct {
		c   *Chunk
		inc int
		idx int
		mb  metaBuf
	}

	// UnsafeRecord represent a chunk record. This is a short-life object which may be used ONLY when ChunkReader is open.
	// If the record time should be longer, the UnsafePayload MUST be copied to another memory.
	UnsafeRecord struct {
		ID            ulid.ULID
		UnsafePayload []byte
	}

	// AppendRecordsResult is used to report the append records operation result
	AppendRecordsResult struct {
		// Written is the number of records added to the chunk
		Written int
		// StartID is the first added record ID
		StartID ulid.ULID
		// LastID is the last added record ID
		LastID ulid.ULID
	}

	metaBuf []byte
	metaRec struct {
		ID     ulid.ULID
		offset int32
		size   int32
	}

	// Config defines the chunk settings
	Config struct {
		NewSize             int64
		MaxChunkSize        int64
		MaxGrowIncreaseSize int64
	}
)

const (
	// new Chunk initial size
	cNewSize = files.BlockSize * 16
	// MaxGrowIncreaseSize specifies that the Chunk size may not be increased more than the MaxGrowIncreaseSize
	cMaxGrowIncreaseSize = files.BlockSize * 256
	// MaxChunkSize defines the maximum Chunk size. No Chunk may exceed the size
	cMaxChunkSize = files.BlockSize * 512 * 1024
	cHeaderSize   = 32
	// cMetaRecordSize is the size of one meta-record
	cMetaRecordSize = 24
)

var hdrVersion = []byte{'S', 'O', 'L', 'A', 'R', 'I', 'S', 1}
var _ iterable.Iterator[UnsafeRecord] = (*ChunkReader)(nil)
var errCorrupted = fmt.Errorf("file chunk corrupted")

func GetDefaultConfig() Config {
	return Config{
		NewSize:             cNewSize,
		MaxChunkSize:        cMaxChunkSize,
		MaxGrowIncreaseSize: cMaxGrowIncreaseSize,
	}
}

func (mb metaBuf) get(idx int) metaRec {
	off := len(mb) - (idx+1)*cMetaRecordSize
	var mr metaRec
	lenID := len(mr.ID)
	// Write 16 bytes of the record ID
	copy(mr.ID[:], mb[off:off+lenID])
	// Write 4 bytes for the record offset from the beginning
	mr.offset = int32(binary.BigEndian.Uint32(mb[off+lenID : off+lenID+4]))
	// Write 4 bytes of the payload size
	mr.size = int32(binary.BigEndian.Uint32(mb[off + +lenID + 4 : off+lenID+8]))
	return mr
}

func (mb metaBuf) put(idx int, mr metaRec) {
	off := len(mb) - (idx+1)*cMetaRecordSize
	lenID := len(mr.ID)
	// Write 16 bytes of the record ID
	copy(mb[off:off+lenID], mr.ID[:])
	// Write 4 bytes for the record offset from the beginning
	binary.BigEndian.PutUint32(mb[off+lenID:off+lenID+4], uint32(mr.offset))
	// Write 4 bytes of the payload size
	binary.BigEndian.PutUint32(mb[off+lenID+4:off+lenID+8], uint32(mr.size))

}

// NewChunk creates new Chunk
func NewChunk(fileName, id string, cfg Config) *Chunk {
	return &Chunk{
		id:     id,
		fn:     fileName,
		cfg:    cfg,
		logger: logging.NewLogger(fmt.Sprintf("chunkfs.Chunk.%s", id)),
	}
}

// String implements fmt.Stringer
func (c *Chunk) String() string {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return fmt.Sprintf("Chunk{id:%s, total:%d, freeOffset:%d}",
		c.id, c.total, c.freeOffset)
}

// Open allows to map the chunk file context to the memory and start working with the chunk
func (c *Chunk) Open(fullCheck bool) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.mmf != nil {
		return nil
	}
	c.logger.Debugf("opening, fullCheck=%t", fullCheck)
	mmf, err := files.NewMMFile(c.fn, c.cfg.NewSize)
	if err != nil {
		return err
	}
	c.mmf = mmf
	err = c.init(fullCheck)
	if err != nil {
		c.close()
	} else {
		c.logger.Debugf("opened size=%d, total=%d, freeOffset=%d", c.mmf.Size(), c.total, c.freeOffset)
	}
	return err
}

func (c *Chunk) init(fullCheck bool) error {
	hdr, err := c.mmf.Buffer(0, cHeaderSize)
	if err != nil {
		return err
	}
	vLen := len(hdrVersion)
	if !bytes.Equal(hdr[:vLen], hdrVersion) {
		// makes everything empty
		copy(hdr[:vLen], hdrVersion)
		// total count
		binary.BigEndian.PutUint32(hdr[vLen:vLen+4], uint32(0))
	}
	c.total = int(binary.BigEndian.Uint32(hdr[vLen : vLen+4]))
	if c.total < 0 {
		return fmt.Errorf("the chunk is corrupted, wrong total=%d: %w", c.total, errCorrupted)
	}
	c.freeOffset = cHeaderSize
	if c.total > 0 {
		mb, err := c.getMetaBuf(int(c.total)-1, 1)
		if err != nil {
			return err
		}
		mr := mb.get(0)
		c.freeOffset = int(mr.offset + mr.size)
	}
	if c.freeOffset < cHeaderSize || int64(c.freeOffset) > c.mmf.Size() {
		return fmt.Errorf("the chunk is corrupted, wrong freeOffset=%d: %w", c.freeOffset, errCorrupted)
	}
	if !fullCheck {
		return nil
	}

	mb, err := c.getMetaBuf(0, int(c.total))
	if err != nil {
		return err
	}
	startOffs := c.freeOffset
	var id ulid.ULID
	pMax := int(c.mmf.Size() - int64(c.total*cMetaRecordSize))
	for i := 0; i < c.total; i++ {
		mr := mb.get(i)
		if mr.ID.Compare(id) < 0 {
			return fmt.Errorf("the record #%d ID=%s is less than the previous one %s: %w", i, mr.ID.String(), id.String(), errCorrupted)
		}
		if int(mr.offset) != startOffs {
			return fmt.Errorf("the record #%d offset=%d is not what expected %d: %w", i, mr.offset, startOffs, errCorrupted)
		}
		id = mr.ID
		startOffs = int(mr.offset + mr.size)
		if startOffs > pMax {
			return fmt.Errorf("the record #%d size=%d exceed the maximum payload value: %w", i, mr.size, errCorrupted)
		}
	}
	return nil
}

// Close implements io.Closer. It allows to close the chunk, so the Append and Read operations will not be available
// after that. All readers must be closed befor the call, otherwise it will be blocked
func (c *Chunk) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.close()
}

func (c *Chunk) isOpened() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.mmf != nil
}

func (c *Chunk) close() error {
	var err error
	if c.mmf != nil {
		c.logger.Debugf("closing")
		err = c.mmf.Close()
		c.mmf = nil
	}
	return err
}

// AppendRecords allows to add new records into the chunk. The chunk size can be extended if the records do not fit into
// the existing chunk. If the chunk reaches its maximum capacity it will not grow anymore. Only some records, that
// fit into the chunk will be written. The result will contain the number of records actually written
func (c *Chunk) AppendRecords(recs []*solaris.Record) (AppendRecordsResult, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.mmf == nil {
		// chunk is closed
		return AppendRecordsResult{}, fmt.Errorf("the chunk %s is closed: %w ", c.fn, errors.ErrClosed)
	}
	n, size := c.writable(recs)
	if n == 0 {
		return AppendRecordsResult{}, nil
	}

	if err := c.growForWrite(int64(size)); err != nil {
		// could not grow the Chunk
		return AppendRecordsResult{}, err
	}
	recs = recs[:n]
	mb, err := c.getMetaBuf(int(c.total)+len(recs)-1, len(recs))
	if err != nil {
		return AppendRecordsResult{}, err
	}

	pOffset := c.freeOffset
	var startID, lastID ulid.ULID
	for i, r := range recs {
		lastID = ulidutils.New()
		if i == 0 {
			startID = lastID
		}
		mb.put(i, metaRec{ID: lastID, offset: int32(pOffset), size: int32(len(r.Payload))})
		pOffset += len(r.Payload)
	}

	pSize := pOffset - c.freeOffset
	pBuf, err := c.mmf.Buffer(int64(c.freeOffset), pSize)
	if err != nil {
		c.logger.Errorf("could not map payload-buffer with offset %d for size=%d: %v", c.freeOffset, pSize, err)
		return AppendRecordsResult{}, fmt.Errorf("could not write data: %w", fmt.Errorf("could not map payload-buffer with offset %d for size=%d: %w", c.freeOffset, pSize, errors.ErrInternal))
	}
	pOffset = 0
	for _, r := range recs {
		copy(pBuf[pOffset:int(pOffset)+len(r.Payload)], r.Payload)
		pOffset += len(r.Payload)
	}

	c.freeOffset += pOffset
	c.total += len(recs)
	// update the header
	hdr, err := c.mmf.Buffer(int64(len(hdrVersion)), 4)
	if err != nil {
		c.logger.Errorf("could not map records counter buffer with offset %d for size=4: %v", len(hdrVersion), err)
		return AppendRecordsResult{}, fmt.Errorf("could not map records counter buffer with offset %d for size=4: %w", c.freeOffset, errors.ErrInternal)
	}
	binary.BigEndian.PutUint32(hdr, uint32(c.total))

	return AppendRecordsResult{Written: n, StartID: startID, LastID: lastID}, nil
}

// getMetaBuf maps the meta-buffer for the index startIdx with ln number of meta-records
func (c *Chunk) getMetaBuf(startIdx, ln int) (metaBuf, error) {
	offs := c.mmf.Size() - int64(startIdx+1)*cMetaRecordSize
	mb, err := c.mmf.Buffer(offs, ln*cMetaRecordSize)
	if err != nil {
		c.logger.Errorf("could not map meta-buffer with offset=%d (idx=%d) for size=%d: %v", offs, startIdx, ln, err)
		err = fmt.Errorf("could not map meta-buffer with offset=%d (idx=%d) for size=%d: %w", offs, startIdx, ln, errors.ErrInternal)
	}
	return mb, err
}

// growForWrite allows to increase the Chunk size when the c.available() becomes >= size
func (c *Chunk) growForWrite(size int64) error {
	avail := c.available()
	if avail >= size {
		return nil
	}

	// check whether we may write at all
	afterWriteSize := c.mmf.Size() - avail + size
	if afterWriteSize > c.cfg.MaxChunkSize {
		// with all the records we will exceed the MaxChunkSize
		return fmt.Errorf("could not write %d bytes, cause the chunks size will be %d, which will exceed the maximum value=%d: %w", size, afterWriteSize, c.cfg.MaxChunkSize, errors.ErrExhausted)
	}

	inc := min(c.cfg.MaxGrowIncreaseSize, c.mmf.Size())
	if avail+inc < size {
		inc = ((size-avail)/files.BlockSize + 1) * files.BlockSize
	}
	newSize := c.mmf.Size() + inc
	if newSize > c.cfg.MaxChunkSize {
		// it should be enough, because we checked the condition above
		newSize = c.cfg.MaxChunkSize
	}

	oldSize := c.mmf.Size()
	if err := c.mmf.Grow(newSize); err != nil {
		return err
	}

	if c.total == 0 {
		// empty Chunk
		return nil
	}

	// now, move meta to the end of the new Chunk
	mSize := int(c.total * cMetaRecordSize)
	mOffset := oldSize - int64(mSize)
	oldMBuf, err := c.mmf.Buffer(mOffset, mSize)
	if err != nil {
		c.logger.Errorf("could not map meta-buffer with offset %d for size=%d: %v", mOffset, mSize, err)
		return fmt.Errorf("could not Grow Chunk: %w", fmt.Errorf("could not map meta-buffer with offset %d for size=%d: %w", mOffset, mSize, errors.ErrInternal))
	}
	newMBuf, err := c.mmf.Buffer(c.mmf.Size()-int64(mSize), mSize)
	if err != nil {
		c.logger.Errorf("could not map meta-buffer with offset %d for size=%d: %v", mOffset, mSize, err)
		return fmt.Errorf("could not Grow Chunk: %w", fmt.Errorf("could not map meta-buffer with offset %d for size=%d: %w", mOffset, mSize, errors.ErrInternal))
	}
	copy(newMBuf, oldMBuf)

	return nil
}

// OpenChunkReader opens new read operation. The function returns ChunkReader, which may be used for reading the chunk
// records. The ChunkReader must be closed. The AppendRecords and Close() operations will be blocked until ALL
// ChunkReaders are closed. So the ChunkReader should be requested for a short period of time and be closed as soon as
// possible
func (c *Chunk) OpenChunkReader(descending bool) (*ChunkReader, error) {
	c.lock.RLock()
	if c.mmf == nil {
		// chunk is closed
		c.lock.RUnlock()
		return nil, fmt.Errorf("the chunk %s is closed: %w ", c.fn, errors.ErrClosed)
	}
	mb, err := c.getMetaBuf(c.total-1, c.total)
	if err != nil {
		c.lock.RUnlock()
		return nil, err
	}

	cr := new(ChunkReader)
	cr.c = c
	cr.inc = 1
	cr.mb = mb

	if descending {
		cr.inc = -1
		cr.idx = int(cr.c.total) - 1
	}
	return cr, nil
}

func (c *Chunk) available() int64 {
	return c.mmf.Size() - int64(c.freeOffset+c.total*cMetaRecordSize)
}

// writable returns the number of records and the total size of the records, that can fit into the
// chunk, even if it will grow.
func (c *Chunk) writable(recs []*solaris.Record) (int, int) {
	maxAvaialbe := int(c.cfg.MaxChunkSize) - c.freeOffset + c.total*cMetaRecordSize
	totalSize := 0
	for i, r := range recs {
		recSize := len(r.Payload) + cMetaRecordSize
		if totalSize+recSize > maxAvaialbe {
			return i, totalSize
		}
		totalSize += recSize
	}
	return len(recs), totalSize
}

func (cr *ChunkReader) HasNext() bool {
	return cr.idx < cr.c.total && cr.idx > -1
}

func (cr *ChunkReader) Next() (UnsafeRecord, bool) {
	if cr.HasNext() {
		mr := cr.mb.get(cr.idx)
		buf, err := cr.c.mmf.Buffer(int64(mr.offset), int(mr.size))
		if err != nil {
			cr.c.logger.Errorf("could not read payload for offset=%d for len=%d: %v", mr.offset, mr.size, err)
			panic(err)
		}
		res := UnsafeRecord{ID: mr.ID, UnsafePayload: buf}
		cr.idx += cr.inc
		return res, true
	}
	return UnsafeRecord{}, false
}

// Close implements io.Closer
func (cr *ChunkReader) Close() error {
	cr.c.lock.RUnlock()
	cr.c = nil
	return nil
}

// SetStartID moves the iterator offset to the position startID. The function returns the number of records
// which will be available for read after the call taking into account the direction of the iterator.
func (cr *ChunkReader) SetStartID(startID ulid.ULID) int {
	res := 0
	if cr.inc == -1 {
		cr.idx = sort.Search(cr.c.total, func(i int) bool {
			return cr.mb.get(i).ID.Compare(startID) > 0
		})
		cr.idx--
		res = cr.idx + 1
	} else {
		cr.idx = sort.Search(cr.c.total, func(i int) bool {
			return cr.mb.get(i).ID.Compare(startID) >= 0
		})
		res = cr.c.total - cr.idx
	}
	return res
}
