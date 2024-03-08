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
	"context"
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	"github.com/solarisdb/solaris/golibs/container/iterable"
	context2 "github.com/solarisdb/solaris/golibs/context"
	"github.com/solarisdb/solaris/pkg/storage"
)

// newMixer returns an iterator which mixes a bunch of iterators around the slice logIDs and mix them together to
// retrieve records either in ascending or descending order.
func newMixer(ctx context.Context, cancel context2.CancelErrFunc, ls storage.Log, baseQuery storage.QueryRecordsRequest, logIDs []string) iterable.Iterator[*solaris.Record] {
	if len(logIDs) == 0 {
		return &iterable.EmptyIterator[*solaris.Record]{}
	}
	mxs := make([]iterable.Iterator[*solaris.Record], len(logIDs))
	pits := make([]*rIterator, len(mxs))
	i := 0

	for _, lid := range logIDs {
		baseQuery.LogID = lid
		pits[i] = newRIterator(ctx, cancel, ls, baseQuery)
		mxs[i] = pits[i]
		i++
	}

	var cmpF iterable.SelectF[*solaris.Record]
	if baseQuery.Descending {
		cmpF = descendingRecords
	} else {
		cmpF = ascendingRecords
	}

	// mixing the iterator until only one left
	for len(mxs) > 1 {
		for i := 0; i < len(mxs)-1; i += 2 {
			m := &iterable.Mixer[*solaris.Record]{}
			m.Init(cmpF, mxs[i], mxs[i+1])
			mxs[i/2] = m
		}
		if len(mxs)&1 == 1 {
			mxs[len(mxs)/2] = mxs[len(mxs)-1]
			mxs = mxs[:len(mxs)/2+1]
		} else {
			mxs = mxs[:len(mxs)/2]
		}
	}
	return mxs[0]
}

func ascendingRecords(r1, r2 *solaris.Record) bool {
	return r1.ID < r2.ID
}

func descendingRecords(r1, r2 *solaris.Record) bool {
	return r1.ID > r2.ID
}
