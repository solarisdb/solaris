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
	"context"
	"github.com/solarisdb/solaris/golibs/cast"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/logging"
	"github.com/solarisdb/solaris/golibs/sss/inmem"
	"github.com/solarisdb/solaris/golibs/strutil"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestReplicator_SimpleUploadDownload(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestReplicator_SimpleUploadDownload")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	r := &Replicator{cc: newChunkAccessor(), storage: inmem.NewStorage(), logger: logging.NewLogger("testReplicator"), fileNameByID: func(v string) string {
		return filepath.Join(dir, v)
	}}

	cID := "1234"
	fn := r.fileNameByID(cID)
	payload := createRandomFile(t, fn)
	assert.Nil(t, r.UploadChunk(context.Background(), cID))
	os.Remove(r.fileNameByID(cID))

	// check the chunk accessor
	r.cc.setDeleting(cID)
	assert.NotNil(t, r.DownloadChunk(context.Background(), cID, 0))
	r.cc.setIdle(cID)

	// will check that the file will be downloaded if it doesn't exist
	assert.Nil(t, r.DownloadChunk(context.Background(), cID, 0))
	buf, err := os.ReadFile(fn)
	assert.Nil(t, err)
	assert.Equal(t, buf, cast.StringToByteArray(payload))

	// will check that if the file exists, it will not be downloaded
	// create the new context
	os.Remove(fn)
	createRandomFile(t, fn)
	assert.Nil(t, r.DownloadChunk(context.Background(), cID, 0))
	buf, err = os.ReadFile(fn)
	assert.Nil(t, err)
	assert.NotEqual(t, buf, cast.StringToByteArray(payload))

	// now force sync
	assert.Nil(t, r.DownloadChunk(context.Background(), cID, RFRemoteSync))
	buf, err = os.ReadFile(fn)
	assert.Nil(t, err)
	assert.Equal(t, buf, cast.StringToByteArray(payload))

	assert.True(t, errors.Is(r.DownloadChunk(context.Background(), "lslsl", RFRemoteSync), errors.ErrNotExist))
}

func TestReplicator_SimpleDelete(t *testing.T) {
	dir, err := os.MkdirTemp("", "TestReplicator_SimpleDelete")
	assert.Nil(t, err)
	defer os.RemoveAll(dir)

	r := &Replicator{cc: newChunkAccessor(), storage: inmem.NewStorage(), logger: logging.NewLogger("testReplicator"), fileNameByID: func(v string) string {
		return filepath.Join(dir, v)
	}}

	cID := "1234"
	fn := r.fileNameByID(cID)
	payload := createRandomFile(t, fn)
	assert.Nil(t, r.UploadChunk(context.Background(), cID))

	// check the chunk accessory
	r.cc.openChunk(context.Background(), cID)
	assert.NotNil(t, r.DeleteChunk(context.Background(), cID, 0))
	assert.Nil(t, r.cc.closeChunk(cID))

	// not both flags
	assert.NotNil(t, r.DeleteChunk(context.Background(), cID, RFRemoteDelete|RFRemoteSync))
	_, err = os.Stat(fn)
	assert.Nil(t, err)

	// delete locally, but upload it remotely
	assert.Nil(t, r.DeleteChunk(context.Background(), cID, RFRemoteSync))
	_, err = os.Stat(fn)
	assert.True(t, errors.Is(err, errors.ErrNotExist))
	assert.Nil(t, r.DownloadChunk(context.Background(), cID, 0))
	buf, err := os.ReadFile(fn)
	assert.Nil(t, err)
	assert.Equal(t, buf, cast.StringToByteArray(payload))

	// delete locally only
	assert.Nil(t, r.DeleteChunk(context.Background(), cID, 0))
	_, err = os.Stat(fn)
	assert.True(t, errors.Is(err, errors.ErrNotExist))

	// could not delete deleted
	assert.NotNil(t, r.DeleteChunk(context.Background(), cID, RFRemoteSync))

	// check the file exists remotely
	assert.Nil(t, r.DownloadChunk(context.Background(), cID, 0))
	buf, err = os.ReadFile(fn)
	assert.Nil(t, err)
	assert.Equal(t, buf, cast.StringToByteArray(payload))

	// delete everywhere
	assert.Nil(t, r.DeleteChunk(context.Background(), cID, RFRemoteDelete))
	assert.NotNil(t, r.DownloadChunk(context.Background(), cID, 0))

	assert.NotNil(t, r.DeleteChunk(context.Background(), cID, RFRemoteDelete))
}

func createRandomFile(t *testing.T, fn string) string {
	f, err := os.Create(fn)
	assert.Nil(t, err)
	defer f.Close()
	s := strutil.RandomString(512)
	_, err = f.Write(cast.StringToByteArray(s))
	assert.Nil(t, err)
	return s
}
