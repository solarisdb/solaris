package buntdb

import (
	"context"
	"github.com/solarisdb/solaris/api/gen/solaris/v1"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/pkg/storage"
	"github.com/solarisdb/solaris/pkg/storage/logfs"
	"github.com/stretchr/testify/assert"
	"maps"
	"testing"
)

func TestStorage_CreateLog(t *testing.T) {
	ctx := context.Background()
	s := getStorage(ctx, t)

	log := &solaris.Log{Tags: map[string]string{"tag1": "val1", "tag2": "val2"}}
	log, err := s.CreateLog(ctx, log)
	assert.Nil(t, err)
	assert.NotEmpty(t, log.ID)
	assert.NotEmpty(t, log.CreatedAt)
	assert.NotEmpty(t, log.UpdatedAt)
}

func TestStorage_UpdateLog(t *testing.T) {
	ctx := context.Background()
	s := getStorage(ctx, t)

	log1 := &solaris.Log{Tags: map[string]string{"tag1": "val1", "tag2": "val2"}}
	log1, err := s.CreateLog(ctx, log1)
	assert.Nil(t, err)

	log2, err := s.GetLogByID(ctx, log1.ID)
	assert.Nil(t, err)
	assert.Equal(t, log2.ID, log1.ID)
	assert.True(t, maps.Equal(log2.Tags, log1.Tags))

	log1.Tags["tag5"] = "val5"
	log2, err = s.UpdateLog(ctx, log1)
	assert.Nil(t, err)
	assert.True(t, maps.Equal(log2.Tags, log1.Tags))
}

func TestStorage_GetLogByID(t *testing.T) {
	ctx := context.Background()
	s := getStorage(ctx, t)

	log1 := &solaris.Log{Tags: map[string]string{"tag1": "val1", "tag2": "val2"}}
	log1, err := s.CreateLog(ctx, log1)
	assert.Nil(t, err)

	log2 := &solaris.Log{Tags: map[string]string{"tag3": "val3", "tag4": "val4"}}
	log2, err = s.CreateLog(ctx, log2)
	assert.Nil(t, err)

	log3, err := s.GetLogByID(ctx, log2.ID)
	assert.Nil(t, err)
	assert.Equal(t, log2.ID, log3.ID)
	assert.True(t, maps.Equal(log2.Tags, log3.Tags))

	log4, err := s.GetLogByID(ctx, log1.ID)
	assert.Nil(t, err)
	assert.Equal(t, log1.ID, log4.ID)
	assert.True(t, maps.Equal(log1.Tags, log4.Tags))
}

func TestStorage_QueryLogsByCondition(t *testing.T) {
	ctx := context.Background()
	s := getStorage(ctx, t)

	log1 := &solaris.Log{Tags: map[string]string{"tag1": "val1", "tag2": "val2"}}
	log1, err := s.CreateLog(ctx, log1)
	assert.Nil(t, err)

	log2 := &solaris.Log{Tags: map[string]string{"tag3": "val3", "tag4": "val4"}}
	log2, err = s.CreateLog(ctx, log2)
	assert.Nil(t, err)

	log3 := &solaris.Log{Tags: map[string]string{"tag3": "val4", "tag4": "val4"}}
	log3, err = s.CreateLog(ctx, log3)
	assert.Nil(t, err)

	qr, err := s.QueryLogs(ctx, storage.QueryLogsRequest{Condition: "tag('tag3') = 'val3' OR tag('tag3') = 'val4' OR tag('tag1') like 'v%1'", Limit: 2})
	assert.Nil(t, err)
	assert.Equal(t, 2, len(qr.Logs))
	assert.Equal(t, int64(3), qr.Total)
	assert.Equal(t, qr.NextPageID, log3.ID)
}

func TestStorage_QueryLogsByIDs(t *testing.T) {
	ctx := context.Background()
	s := getStorage(ctx, t)

	log1 := &solaris.Log{}
	log1, err := s.CreateLog(ctx, log1)
	assert.Nil(t, err)

	log2 := &solaris.Log{}
	log2, err = s.CreateLog(ctx, log2)
	assert.Nil(t, err)

	log3 := &solaris.Log{}
	log3, err = s.CreateLog(ctx, log3)
	assert.Nil(t, err)

	qr, err := s.QueryLogs(ctx, storage.QueryLogsRequest{IDs: []string{log1.ID, log2.ID, log3.ID}, Condition: "must not matter", Limit: 2})
	assert.Nil(t, err)
	assert.Equal(t, 2, len(qr.Logs))
	assert.Equal(t, int64(3), qr.Total)
	assert.Equal(t, qr.NextPageID, log3.ID)
}

func TestStorage_DeleteLogsByCondition(t *testing.T) {
	ctx := context.Background()
	s := getStorage(ctx, t)

	log1 := &solaris.Log{Tags: map[string]string{"tag1": "val1", "tag2": "val2"}}
	log1, err := s.CreateLog(ctx, log1)
	assert.Nil(t, err)

	log2 := &solaris.Log{Tags: map[string]string{"tag3": "val3", "tag4": "val4"}}
	log2, err = s.CreateLog(ctx, log2)
	assert.Nil(t, err)

	log3 := &solaris.Log{Tags: map[string]string{"tag3": "val4", "tag4": "val4"}}
	log3, err = s.CreateLog(ctx, log3)
	assert.Nil(t, err)

	dr, err := s.DeleteLogs(ctx, storage.DeleteLogsRequest{Condition: "tag('tag3') = 'val4' AND tag('tag4') like 'v%'"})
	assert.Nil(t, err)
	assert.Equal(t, int64(1), dr.Total)

}

func TestStorage_DeleteLogsByConditionMarkOnly(t *testing.T) {
	ctx := context.Background()
	s := getStorage(ctx, t)

	log := &solaris.Log{Tags: map[string]string{"tag1": "val1", "tag2": "val2"}}
	log, err := s.CreateLog(ctx, log)
	assert.Nil(t, err)

	dr, err := s.DeleteLogs(ctx, storage.DeleteLogsRequest{Condition: "tag('tag1') = 'val1'", MarkOnly: true})
	assert.Nil(t, err)
	assert.Equal(t, int64(1), dr.Total)

	dr, err = s.DeleteLogs(ctx, storage.DeleteLogsRequest{Condition: "tag('tag1') = 'val1'", MarkOnly: true})
	assert.Nil(t, err)
	assert.Equal(t, int64(0), dr.Total)

	log, err = s.GetLogByID(ctx, log.ID)
	assert.ErrorIs(t, err, errors.ErrNotExist)

	dr, err = s.DeleteLogs(ctx, storage.DeleteLogsRequest{Condition: "tag('tag1') = 'val1'"})
	assert.Nil(t, err)
	assert.Equal(t, int64(1), dr.Total)

	dr, err = s.DeleteLogs(ctx, storage.DeleteLogsRequest{Condition: "tag('tag1') = 'val1'"})
	assert.Nil(t, err)
	assert.Equal(t, int64(0), dr.Total)
}

func TestStorage_DeleteLogsByIDs(t *testing.T) {
	ctx := context.Background()
	s := getStorage(ctx, t)

	log1 := &solaris.Log{Tags: map[string]string{"tag1": "val1", "tag2": "val2"}}
	log1, err := s.CreateLog(ctx, log1)
	assert.Nil(t, err)

	log2 := &solaris.Log{Tags: map[string]string{"tag3": "val3", "tag4": "val4"}}
	log2, err = s.CreateLog(ctx, log2)
	assert.Nil(t, err)

	log3 := &solaris.Log{Tags: map[string]string{"tag3": "val4", "tag4": "val4"}}
	log3, err = s.CreateLog(ctx, log3)
	assert.Nil(t, err)

	dr, err := s.DeleteLogs(ctx, storage.DeleteLogsRequest{IDs: []string{log2.ID, log3.ID}})
	assert.Nil(t, err)
	assert.Equal(t, int64(2), dr.Total)
}

func TestStorage_DeleteLogByIDsWithChunks(t *testing.T) {
	ctx := context.Background()
	s := getStorage(ctx, t)

	log1 := &solaris.Log{}
	log1, err := s.CreateLog(ctx, log1)
	assert.Nil(t, err)

	cis1 := []logfs.ChunkInfo{{ID: "2"}, {ID: "1"}}
	err = s.UpsertChunkInfos(ctx, log1.ID, cis1)
	assert.Nil(t, err)

	log2 := &solaris.Log{}
	log2, err = s.CreateLog(ctx, log2)
	assert.Nil(t, err)

	cis2 := []logfs.ChunkInfo{{ID: "3"}, {ID: "4"}}
	err = s.UpsertChunkInfos(ctx, log2.ID, cis2)
	assert.Nil(t, err)

	cis3, err := s.GetChunks(ctx, log1.ID)
	assert.Nil(t, err)
	assert.Equal(t, len(cis1), len(cis3))

	cis4, err := s.GetChunks(ctx, log2.ID)
	assert.Nil(t, err)
	assert.Equal(t, len(cis2), len(cis4))

	cnt, err := s.DeleteLogs(ctx, storage.DeleteLogsRequest{IDs: []string{log2.ID}})
	assert.Nil(t, err)
	assert.Equal(t, int64(1), cnt.Total)

	cis3, err = s.GetChunks(ctx, log1.ID)
	assert.Nil(t, err)
	assert.Equal(t, len(cis1), len(cis3))

	cis4, err = s.GetChunks(ctx, log2.ID)
	assert.ErrorIs(t, err, errors.ErrNotExist)
}

func TestStorage_GetLastChunk(t *testing.T) {
	ctx := context.Background()
	s := getStorage(ctx, t)

	_, err := s.GetLastChunk(ctx, "noID")
	assert.ErrorIs(t, err, errors.ErrNotExist)

	log := &solaris.Log{}
	log, err = s.CreateLog(ctx, log)
	assert.Nil(t, err)

	_, err = s.GetLastChunk(ctx, log.ID)
	assert.ErrorIs(t, err, errors.ErrNotExist)

	cis := []logfs.ChunkInfo{{ID: "2"}, {ID: "1"}}
	err = s.UpsertChunkInfos(ctx, log.ID, cis)
	assert.Nil(t, err)

	ci, err := s.GetLastChunk(ctx, log.ID)
	assert.Nil(t, err)
	assert.Equal(t, cis[0].ID, ci.ID)
}

func TestStorage_GetChunks(t *testing.T) {
	ctx := context.Background()
	s := getStorage(ctx, t)

	log := &solaris.Log{}
	log, err := s.CreateLog(ctx, log)
	assert.Nil(t, err)

	cis1 := []logfs.ChunkInfo{{ID: "2"}, {ID: "1"}}
	err = s.UpsertChunkInfos(ctx, log.ID, cis1)
	assert.Nil(t, err)

	cis2, err := s.GetChunks(ctx, log.ID)
	assert.Nil(t, err)
	assert.Equal(t, len(cis1), len(cis2))
}

func TestStorage_UpsertChunkInfos(t *testing.T) {
	ctx := context.Background()
	s := getStorage(ctx, t)

	_, err := s.GetChunks(ctx, "noID")
	assert.ErrorIs(t, err, errors.ErrNotExist)

	log := &solaris.Log{}
	log, err = s.CreateLog(ctx, log)
	assert.Nil(t, err)

	cis1 := []logfs.ChunkInfo{{ID: "2"}, {ID: "1"}}
	err = s.UpsertChunkInfos(ctx, log.ID, cis1)
	assert.Nil(t, err)

	cis2, err := s.GetChunks(ctx, log.ID)
	assert.Nil(t, err)
	assert.Equal(t, len(cis1), len(cis2))

	cis3 := []logfs.ChunkInfo{{ID: "2"}, {ID: "1"}, {ID: "3"}}
	err = s.UpsertChunkInfos(ctx, log.ID, cis3)
	assert.Nil(t, err)

	cis4, err := s.GetChunks(ctx, log.ID)
	assert.Nil(t, err)
	assert.Equal(t, len(cis3), len(cis4))
}

func TestStorage_DeleteLogChunks(t *testing.T) {
	ctx := context.Background()
	s := getStorage(ctx, t)

	log1 := &solaris.Log{}
	log1, err := s.CreateLog(ctx, log1)
	assert.Nil(t, err)

	cis1 := []logfs.ChunkInfo{{ID: "2"}, {ID: "1"}}
	err = s.UpsertChunkInfos(ctx, log1.ID, cis1)
	assert.Nil(t, err)

	log2 := &solaris.Log{}
	log2, err = s.CreateLog(ctx, log2)
	assert.Nil(t, err)

	cis2 := []logfs.ChunkInfo{{ID: "3"}, {ID: "4"}}
	err = s.UpsertChunkInfos(ctx, log2.ID, cis2)
	assert.Nil(t, err)

	cnt, err := s.DeleteLogs(ctx, storage.DeleteLogsRequest{IDs: []string{log2.ID}})
	assert.Nil(t, err)
	assert.Equal(t, int64(1), cnt.Total)
}

func getStorage(ctx context.Context, t *testing.T) *Storage {
	//s := NewStorage(Config{DBFilePath: "/tmp/solaris_test.db"})
	s := NewStorage(Config{DBFilePath: ""})
	assert.Nil(t, s.Init(ctx))

	tx := mustBeginTx(s.db, true)
	assert.Nil(t, tx.DeleteAll())
	assert.Nil(t, tx.Commit())
	return s
}
