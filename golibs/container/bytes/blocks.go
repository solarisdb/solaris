// Copyright 2023 The acquirecloud Authors
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
package bytes

import (
	"fmt"
	"github.com/solarisdb/solaris/golibs/errors"
	"os"
	"sync"
	"sync/atomic"
)

type (
	// Blocks struct allows to organize a byte-blocks storage on top of a Buffer storage. The blocks
	// have a constant size and the Blocks allow to allocate (ArrangeBlock) or free previously
	// allocated blocks.
	//
	// Blocks split bts on to set of blocks called segment. Every segment contains fixed
	// number of blocks. First block in each segment is called header, and it contains information about
	// allocated blocks in the segment. Header contains blkSize*8 bits, so one segment can contain
	// blkSize*8 blocks + 1 block for the header.
	Blocks struct {
		// blkSize is the size of one block. If less than 4096, then the blkSize should
		// multiply on an integer to get 4096. If equal or greater 4096, it should
		// be divided on 4096 with the reminder 0.
		blkSize int

		// blksInSegm contains number of blocks in one segment (excluding header block)
		// so the segment size in bytes is (blksInSegm+1)*blkSize
		blksInSegm int

		// segments contains number of segments available
		segments int

		// freeIdx contains absolute index in bts (falls in one of the headers) where
		// first empty block could be found
		freeIdx int

		// bts is underlying Buffer storage where the data is persisted
		bts Buffer

		lock sync.Mutex

		available int32
	}
)

// GetBlocksInSegment returns the absolute number (including header) of blocks that may
// be in a segment with the size of each block = blkSize.
// It returns -1 if blkSize invalid (not acceptable)
func GetBlocksInSegment(blkSize int) int {
	if blkSize <= 0 {
		return -1
	}
	pageSize := os.Getpagesize()
	if blkSize < pageSize {
		// the blkSize must be power of 2
		if blkSize&(blkSize-1) != 0 {
			return -1
		}
	} else if blkSize%pageSize != 0 {
		return -1
	}

	return (blkSize * 8) + 1
}

// NewBlocks creates the new Blocks on top of Bytes storage. The storage should
// be opened, and it has to have appropriate size, so the blocks could properly fit there
// Params:
//
//		bs 	- specifies one block size
//		bts - the Bytes storage
//	 fit - specifies the bts.Size() must match exactly to the block storage expectations.
//				if the fit is false, the bts storage can be larger than expected.
func NewBlocks(bs int, bts Buffer, fit bool) (*Blocks, error) {
	// get absolute number of blocks in a segment
	blksInSegm := GetBlocksInSegment(bs)
	if bs < 0 {
		return nil, fmt.Errorf("incorrect block size=%d, should multiple on naturanl integer to get %d: %w ", bs, os.Getpagesize(), errors.ErrInvalid)
	}

	// a segment size in bytes
	segmSize := int64(blksInSegm * bs)
	size := bts.Size()

	// should be at least one segment
	if size < segmSize || (fit && size%segmSize != 0) {
		return nil, fmt.Errorf("incorrect byte storage size=%d, should be divided on segment size=%d with no reminder: %w", size, segmSize, errors.ErrInvalid)
	}

	bks := new(Blocks)
	bks.blkSize = bs
	bks.blksInSegm = blksInSegm - 1
	bks.segments = int(size / segmSize)
	bks.bts = bts
	bks.freeIdx = 0

	return bks, bks.initAvailabe()
}

// Close allows to close the Blocks storage
func (bks *Blocks) Close() error {
	return bks.bts.Close()
}

// Block returns a block value, or if the index is out of range, or access to the block
// is not possible
func (bks *Blocks) Block(idx int) ([]byte, error) {
	segm := idx / bks.blksInSegm
	if segm >= bks.segments || idx < 0 {
		return nil, fmt.Errorf("the block idx=%d is out of bounds [0..%d): %w", idx, bks.blksInSegm*bks.segments, errors.ErrInvalid)
	}

	offs := int64((idx + segm + 1) * bks.blkSize)
	return bks.bts.Buffer(offs, bks.blkSize)
}

// ArrangeBlock arranges a new empty block and return its index.
// The function returns ErrExhausted error if no available blocks in the storage anymore
func (bks *Blocks) ArrangeBlock() (int, error) {
	bks.lock.Lock()
	freeSegm := bks.freeIdx / ((bks.blksInSegm + 1) * bks.blkSize)
	for freeSegm < bks.segments {
		pos := bks.freeIdx % bks.blkSize
		buf, err := bks.bts.Buffer(int64(bks.freeIdx-pos), bks.blkSize)
		if err != nil {
			bks.lock.Unlock()
			return 0, err
		}

		for pos < len(buf) {
			if buf[pos] != 0xFF {
				for j := uint(0); j < 8; j++ {
					if buf[pos]&(1<<j) == 0 {
						buf[pos] |= 1 << j
						atomic.AddInt32(&bks.available, -1)
						bks.lock.Unlock()
						return freeSegm*bks.blksInSegm + pos*8 + int(j), nil
					}
				}
			}
			pos++
			bks.freeIdx++
		}
		freeSegm++
		bks.freeIdx = freeSegm * ((bks.blksInSegm + 1) * bks.blkSize)
	}
	bks.lock.Unlock()
	return 0, errors.ErrExhausted
}

// FreeBlock releases the block by its idx
func (bks *Blocks) FreeBlock(idx int) error {
	offs, fidx, bit := bks.getBlockIdxInHdr(idx)
	if offs < 0 {
		return fmt.Errorf("the block with idx=%d has an offset=%d which is out of bounds: %w", idx, offs, errors.ErrInvalid)
	}

	buf, err := bks.bts.Buffer(offs, bks.blkSize)
	if err != nil {
		return err
	}

	bks.lock.Lock()
	if (buf[fidx] & (1 << bit)) == 0 {
		bks.lock.Unlock()
		return fmt.Errorf("the bock with idx=%d is not allocated(free): %w", idx, errors.ErrNotExist)
	}
	buf[fidx] = buf[fidx] & (0xFF ^ (1 << bit))
	atomic.AddInt32(&bks.available, 1)

	// now adjust the freeIdx, which is absolute
	idx = int(offs) + fidx
	if bks.freeIdx > idx {
		bks.freeIdx = idx
	}

	bks.lock.Unlock()
	return nil
}

// Segments returns number of segments available
func (bks *Blocks) Segments() int {
	return bks.segments
}

// Completion returns (Count() - Available())/Count() the value in [0..1]
func (bks *Blocks) Completion() float32 {
	cnt := bks.Count()
	if cnt > 0 {
		return float32(cnt-bks.Available()) / float32(cnt)
	}
	return -1.0
}

// Count returns total number of blocks
func (bks *Blocks) Count() int {
	return bks.segments * bks.blksInSegm
}

// Available returns number of free blocks.
func (bks *Blocks) Available() int {
	return int(atomic.LoadInt32(&bks.available))
}

// Bytes returns underlying Bytes storage
func (bks *Blocks) Bytes() Buffer {
	return bks.bts
}

func (bks *Blocks) String() string {
	return fmt.Sprintf("{segms: %d, count: %d, available: %d, bts: %s}", bks.segments, bks.Count(), bks.Available(), bks.bts)
}

func (bks *Blocks) initAvailabe() error {
	cnt := 0
	for s := 0; s < bks.segments; s++ {
		offs := int64(s * (bks.blksInSegm + 1) * bks.blkSize)
		buf, err := bks.bts.Buffer(offs, bks.blkSize)
		if err != nil {
			return err
		}
		for _, v := range buf {
			if v != 0xFF {
				for j := uint(0); j < 8; j++ {
					if v&(1<<j) == 0 {
						cnt++
					}
				}
			}
		}
	}
	bks.available = int32(cnt)
	return nil
}

// getBlockIdxInHdr returns the block record allocation coordinates
// (offset in bts, offset in the block and the bit) by the block index idx.
// if the offset in btx is -1, then the idx is out of bounds
func (bks *Blocks) getBlockIdxInHdr(idx int) (int64, int, uint) {
	segm := idx / bks.blksInSegm
	if segm >= bks.segments || idx < 0 {
		return -1, -1, 0
	}

	offs := int64(segm * (bks.blksInSegm + 1) * bks.blkSize)
	bidx := idx % bks.blksInSegm
	fidx := bidx / 8
	bit := uint(bidx % 8)
	return offs, fidx, bit
}
