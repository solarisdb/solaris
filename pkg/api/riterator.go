package api

import (
	"context"
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	"github.com/solarisdb/solaris/golibs/container/iterable"
	context2 "github.com/solarisdb/solaris/golibs/context"
	"github.com/solarisdb/solaris/golibs/ulidutils"
	"github.com/solarisdb/solaris/pkg/storage"
)

// rIterator is the implementation of iterable.Iterator for a specific log ID
type rIterator struct {
	ctx context.Context
	cf  context2.CancelErrFunc
	ls  storage.Log
	// baseQuery contains some parameters like condition, direction etc.
	baseQuery storage.QueryRecordsRequest
	nextID    string // the ID of record will be returned next, if any
	buf       []*solaris.Record
	bPos      int
	eof       bool
}

var _ iterable.Iterator[*solaris.Record] = (*rIterator)(nil)

func newRIterator(ctx context.Context, cf context2.CancelErrFunc, ls storage.Log, baseQuery storage.QueryRecordsRequest) *rIterator {
	ri := new(rIterator)
	ri.ctx = ctx
	ri.cf = cf
	ri.ls = ls
	ri.baseQuery = baseQuery
	ri.nextID = baseQuery.StartID
	return ri
}

func (ri *rIterator) HasNext() bool {
	err := ri.fillBuf()
	return err == nil && !ri.eof
}

func (ri *rIterator) Next() (*solaris.Record, bool) {
	for !ri.eof && ri.ctx.Err() == nil {
		if ri.bPos < len(ri.buf) {
			res := ri.buf[ri.bPos]
			if ri.bPos < len(ri.buf)-1 {
				ri.nextID = ri.buf[ri.bPos+1].ID
			} else {
				if ri.baseQuery.Descending {
					ri.nextID = ulidutils.PrevID(res.ID)
				} else {
					ri.nextID = ulidutils.NextID(res.ID)
				}
			}
			ri.bPos++
			return res, true
		}
		if ri.fillBuf() != nil {
			break
		}
	}
	return nil, false
}

// Reset provides Reseter interface implementation to have mixers
// be happy about the Reset()
func (ri *rIterator) Reset() error {
	ri.eof = false
	return nil
}

func (ri *rIterator) fillBuf() error {
	if ri.bPos < len(ri.buf) {
		return ri.ctx.Err()
	}

	q := ri.baseQuery
	q.Limit = min(100, ri.baseQuery.Limit)
	q.StartID = ri.nextID
	ri.buf = nil
	mr, err := ri.ls.QueryRecords(ri.ctx, q)
	if err != nil {
		ri.cf(err) // cancel the context ctx
		ri.eof = true
		return err
	}
	if mr != nil {
		ri.buf = mr
	}
	ri.bPos = 0
	ri.eof = ri.bPos >= len(ri.buf)
	return nil
}

// Close implements io.Closer
func (ri *rIterator) Close() error {
	return nil
}
