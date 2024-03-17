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
	"github.com/solarisdb/solaris/golibs/errors"
	"slices"
	"sort"
	"sync"
)

type testLogsMetaStorage struct {
	lock sync.Mutex
	logs map[string][]ChunkInfo
}

func newTestLogsMetaStorage() *testLogsMetaStorage {
	lms := new(testLogsMetaStorage)
	lms.logs = make(map[string][]ChunkInfo)
	return lms
}

func (lms *testLogsMetaStorage) GetLastChunk(_ context.Context, logID string) (ChunkInfo, error) {
	lms.lock.Lock()
	defer lms.lock.Unlock()
	cis, ok := lms.logs[logID]
	if !ok {
		return ChunkInfo{}, errors.ErrNotExist
	}
	return cis[len(cis)-1], nil
}

func (lms *testLogsMetaStorage) GetChunks(ctx context.Context, logID string) ([]ChunkInfo, error) {
	lms.lock.Lock()
	defer lms.lock.Unlock()
	cis, ok := lms.logs[logID]
	if !ok {
		return nil, errors.ErrNotExist
	}
	return cis, nil
}

func (lms *testLogsMetaStorage) UpsertChunkInfos(ctx context.Context, logID string, cis []ChunkInfo) error {
	if len(cis) == 0 {
		return nil
	}
	lms.lock.Lock()
	defer lms.lock.Unlock()
	sort.Slice(cis, func(i, j int) bool {
		return cis[i].ID < cis[j].ID
	})
	ecis, ok := lms.logs[logID]
	if !ok {
		ecis = slices.Clone(cis)
		lms.logs[logID] = ecis
		return nil
	}
	m := map[string]ChunkInfo{}
	for _, ci := range cis {
		m[ci.ID] = ci
	}

	for i, ci := range ecis {
		if v, ok := m[ci.ID]; ok {
			ecis[i] = v
			delete(m, ci.ID)
		}
	}

	for _, ci := range cis {
		if _, ok := m[ci.ID]; ok {
			ecis = append(ecis, ci)
		}
	}
	lms.logs[logID] = ecis
	return nil
}
