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
	"fmt"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/files"
	"github.com/solarisdb/solaris/golibs/logging"
	"github.com/solarisdb/solaris/golibs/sss"
	"io"
	"os"
	"path/filepath"
)

// Replicator struct implements the object which controls the state of the local file-system and allows to move
// the chunks from the local FS to a remote storage forth and back.
type Replicator struct {
	cc           *chunkAccessor
	fileNameByID func(id string) string
	storage      sss.Storage
	logger       logging.Logger
}

const (
	RFRemoteDelete = 1
	RFRemoteSync   = 1 << 1
)

// UploadChunk moves the chunk with ID from the local FS to the remote storage.
func (r *Replicator) UploadChunk(ctx context.Context, cID string) error {
	if err := r.cc.setWriting(ctx, cID); err != nil {
		return err
	}
	defer r.cc.setIdle(cID)
	return r.zipAndUploadChunk(ctx, cID)
}

// DownloadChunk allows to download the chunk by its ID from the remote storage to the local FS.
// The RFRemoteSync flag specifies whether the chunk will be downloaded even if the chunk file already
// exists on the file system. If the chunk file doesn't exist locally, it will be downloaded anyway from the
// remote storage
func (r *Replicator) DownloadChunk(ctx context.Context, cID string, flags int) error {
	if err := r.cc.setWriting(ctx, cID); err != nil {
		return err
	}
	defer r.cc.setIdle(cID)

	fn := r.fileNameByID(cID)
	if flags&RFRemoteSync == 0 {
		if _, err := os.Stat(fn); err == nil {
			// the file is here, do nothing then
			return nil
		}
	}

	r.logger.Debugf("downolading chunk cID=%s from remote storage", cID)
	zfn := fn + ".zip"
	defer os.Remove(zfn)
	if err := r.downloadZip(ctx, cID, zfn); err != nil {
		return err
	}
	return r.unzip(zfn, fn)
}

// DeleteChunk allows to delete the chunk locally. The function may upload the chunk to the remote storage
// before being deleted (the flags&RFRemoteSync != 0), or to remove the chunk locally only (no flags required) and
// remove it locally and remotely (flags&RFRemoteDelete != 0)
func (r *Replicator) DeleteChunk(ctx context.Context, cID string, flags int) error {
	if flags&RFRemoteDelete != 0 && flags&RFRemoteSync != 0 {
		return fmt.Errorf("the flags RFRemoteDelete and RFRemoteSync cannot be specified both when a chunk is removed. cID=%s: %w", cID, errors.ErrInvalid)
	}
	if ok := r.cc.setDeleting(cID); !ok {
		return fmt.Errorf("the chunk cID=%s, is used and cannot be deleted at the time: %w", cID, errors.ErrConflict)
	}
	defer r.cc.setIdle(cID)
	r.logger.Debugf("deleting chunk cID=%s, flags=%d", cID, flags)
	var resErr error
	if flags&RFRemoteSync != 0 {
		if err := r.zipAndUploadChunk(ctx, cID); err != nil {
			r.logger.Warnf("error while syncing chunk cID=%s, flags=%d to remote: %s", cID, flags, err)
			resErr = err
		}
	}

	fn := r.fileNameByID(cID)
	if err := os.Remove(fn); err != nil && !errors.Is(err, errors.ErrNotExist) {
		r.logger.Warnf("error while deleting cID=%s, fn=%s: %s", cID, fn, err)
		resErr = err
	}

	if flags&RFRemoteDelete != 0 {
		err := r.storage.Delete(ctx, getStorageKey(cID))
		if err != nil {
			r.logger.Warnf("could not delete the chunk cID=%s remotely: %s", cID, err)
			resErr = err
		}
	}

	return resErr
}

func (r *Replicator) zipAndUploadChunk(ctx context.Context, cID string) error {
	fn := r.fileNameByID(cID)
	zfn := fn + ".zip"
	defer os.Remove(zfn)

	if err := zipFile(cID, fn, zfn); err != nil {
		return err
	}

	// now the zip file itself
	zf, err := os.Open(zfn)
	if err != nil {
		return err
	}
	defer zf.Close()

	return r.storage.Put(ctx, getStorageKey(cID), zf)
}

func zipFile(cID, fn, zfn string) error {
	zw, err := files.NewZipWriter(zfn)
	if err != nil {
		return err
	}
	defer zw.Close()

	// the chunk file
	f, err := os.Open(fn)
	if err != nil {
		return err
	}
	defer f.Close()

	// writer to the zip file
	w, err := zw.Create(cID)
	if err != nil {
		return err
	}
	if _, err := io.Copy(w, f); err != nil {
		return err
	}
	return nil
}

func getStorageKey(cID string) string {
	return filepath.Join("/", cID[len(cID)-2:], cID)
}

func (r *Replicator) downloadZip(ctx context.Context, cID, zfn string) error {
	rdr, err := r.storage.Get(ctx, getStorageKey(cID))
	if err != nil {
		return err
	}
	defer rdr.Close()

	f, err := os.Create(zfn)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, rdr)
	return err
}

func (r *Replicator) unzip(zfn, fn string) error {
	zit, err := files.NewZipIterator(zfn)
	if err != nil {
		return err
	}
	defer zit.Close()

	zf := zit.Next()
	if zf == nil {
		return fmt.Errorf("the downloaded chunk for the file=%s is corrupted: %w", zfn, errors.ErrDataLoss)
	}
	it, err := zf.Open()
	if err != nil {
		return err
	}
	defer it.Close()

	_ = os.Remove(fn)
	f, err := os.Create(fn)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, it)
	return err
}
