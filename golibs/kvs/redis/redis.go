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
package redis

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/solarisdb/solaris/golibs/cast"
	"github.com/solarisdb/solaris/golibs/container/iterable"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/kvs"
	"github.com/solarisdb/solaris/golibs/kvs/genproto/golibskvspb/v1"
	"github.com/solarisdb/solaris/golibs/ulidutils"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

type (
	client struct {
		rdb *redis.Client
	}

	keysIterator struct {
		si  *redis.ScanIterator
		val *string
	}
)

func New(opts *redis.Options) kvs.Storage {
	rdb := redis.NewClient(opts)
	return &client{rdb: rdb}
}

func (c *client) Create(ctx context.Context, record kvs.Record) (string, error) {
	record.Version = ulidutils.NewID()
	buf := rec2db(&record)
	ok, err := c.rdb.SetNX(ctx, rKey(record.Key), buf, expiration(record.ExpiresAt, time.Now())).Result()
	if err != nil {
		return "", checkErr(err)
	}
	if !ok {
		return "", errors.ErrExist
	}
	return record.Version, nil
}

func (c *client) Get(ctx context.Context, key string) (kvs.Record, error) {
	val, err := c.rdb.Get(ctx, rKey(key)).Result()
	if err != nil {
		return kvs.Record{}, checkErr(err)
	}

	r := db2rec(cast.StringToByteArray(val))
	r.Key = key
	return r, nil
}

func (c *client) GetMany(ctx context.Context, keys ...string) ([]*kvs.Record, error) {
	res, err := c.rdb.MGet(ctx, rKeys(keys)...).Result()
	if err != nil {
		return nil, checkErr(err)
	}
	result := make([]*kvs.Record, len(keys))
	for idx, val := range res {
		if val == nil {
			continue
		}
		r := db2rec(cast.StringToByteArray(val.(string)))
		r.Key = keys[idx]
		result[idx] = &r
	}
	return result, nil
}

func (c *client) Put(ctx context.Context, record kvs.Record) (kvs.Record, error) {
	record.Version = ulidutils.NewID()
	buf := rec2db(&record)
	_, err := c.rdb.Set(ctx, rKey(record.Key), buf, expiration(record.ExpiresAt, time.Now())).Result()
	return record, checkErr(err)
}

func (c *client) PutMany(ctx context.Context, records []kvs.Record) error {
	mset := make([]string, 0, len(records)*2)
	for _, r := range records {
		if r.ExpiresAt != nil {
			mset = nil
			break
		}
		mset = append(mset, rKey(r.Key))
		mset = append(mset, cast.ByteArrayToString(rec2db(&r)))
	}
	if len(mset) > 0 {
		_, err := c.rdb.MSet(ctx, mset).Result()
		return checkErr(err)
	}
	for _, r := range records {
		_, err := c.Put(ctx, r)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *client) CasByVersion(ctx context.Context, record kvs.Record) (kvs.Record, error) {
	key := rKey(record.Key)
	err := c.rdb.Watch(ctx, func(tx *redis.Tx) error {
		val, err := tx.Get(ctx, key).Result()
		if err != nil {
			return checkErr(err)
		}
		r := db2rec(cast.StringToByteArray(val))
		if r.Version != record.Version {
			return errors.ErrConflict
		}
		record.Version = ulidutils.NewID()
		buf := rec2db(&record)
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			_, err2 := pipe.Set(ctx, key, buf, expiration(record.ExpiresAt, time.Now())).Result()
			return err2
		})
		return err
	}, key)
	return record, err
}

func (c *client) Delete(ctx context.Context, key string) error {
	cnt, err := c.rdb.Del(ctx, rKey(key)).Result()
	if err != nil {
		return checkErr(err)
	}
	if cnt == 0 {
		return errors.ErrNotExist
	}
	return nil
}

// WaitForVersionChange is naive optimisitc-spin implementation of the function here. If we
// find better way (key notifications), let's use them in the future. So far it
// should be good enough
func (c *client) WaitForVersionChange(ctx context.Context, key, ver string) error {
	timeout := time.Millisecond * 2
	for {
		timeout *= 2
		if timeout > time.Millisecond*100 {
			timeout = time.Millisecond * 2
		}
		r, err := c.Get(ctx, key)
		if err == errors.ErrNotExist {
			return err
		}
		if err != nil {
			return err
		}
		if r.Version != ver {
			return nil
		}
		tmr := time.NewTimer(timeout)
		select {
		case <-ctx.Done():
			if !tmr.Stop() {
				<-tmr.C
			}
			return ctx.Err()
		case <-tmr.C:
		}
	}
}

// ListKeys allows to read the keys by the pattern provided.
func (c *client) ListKeys(ctx context.Context, pattern string) (iterable.Iterator[string], error) {
	si := c.rdb.Scan(ctx, 0, rKey(pattern), 1000).Iterator()
	return &keysIterator{si: si}, nil
}

func (c *client) Close() error {
	return c.rdb.Close()
}

func checkErr(err error) error {
	if err == nil {
		return nil
	}
	if err.Error() == "redis: nil" {
		return errors.ErrNotExist
	}
	return err
}

func expiration(eat *time.Time, curT time.Time) time.Duration {
	expiration := time.Duration(0)
	if eat != nil {
		expiration = (*eat).Sub(curT)
		if expiration < time.Millisecond {
			expiration = time.Millisecond
		}
	}
	return expiration
}

func rKeys(keys []string) []string {
	res := make([]string, len(keys))
	for idx, key := range keys {
		res[idx] = rKey(key)
	}
	return res
}

func rKey(key string) string {
	for len(key) > 0 && key[0] == '/' {
		key = key[1:]
	}
	return fmt.Sprintf("/kvs/%s", key)
}

func key(rKey string) string {
	if len(rKey) > 5 {
		return rKey[5:]
	}
	return ""
}

func rec2db(r *kvs.Record) []byte {
	if r == nil {
		panic("rec2db: record cannot be nil")
	}
	buf, err := proto.Marshal(Record2protoRecord(r))
	if err != nil {
		panic(fmt.Sprintf("could not marshal record r=%v: %s", r, err))
	}
	return buf
}

func db2rec(buf []byte) kvs.Record {
	var r golibskvspb.Record
	err := proto.Unmarshal(buf, &r)
	if err != nil {
		panic(fmt.Sprintf("could not unmarshal record: %s", err))
	}
	return ProtoRecord2Record(&r)
}

var _ iterable.Iterator[string] = (*keysIterator)(nil)

func (k *keysIterator) HasNext() bool {
	if k.val == nil && k.si.Next(context.Background()) {
		k.val = cast.Ptr(key(k.si.Val()))
	}
	return k.val != nil
}

func (k *keysIterator) Next() (string, bool) {
	if k.HasNext() {
		res := *k.val
		k.val = nil
		return res, true
	}
	return "", false
}

func (k *keysIterator) Close() error {
	k.si = nil
	k.val = nil
	return nil
}

func Record2protoRecord(r *kvs.Record) *golibskvspb.Record {
	res := &golibskvspb.Record{
		Key:     r.Key,
		Value:   r.Value,
		Version: r.Version,
	}
	if r.ExpiresAt != nil {
		res.ExpiresAt = timestamppb.New((*r.ExpiresAt).UTC())
	}
	return res
}

func ProtoRecord2Record(r *golibskvspb.Record) kvs.Record {
	if r == nil {
		return kvs.Record{}
	}
	res := kvs.Record{
		Key:     r.Key,
		Value:   r.Value,
		Version: r.Version,
	}
	if r.ExpiresAt != nil {
		t := r.ExpiresAt.AsTime()
		res.ExpiresAt = cast.Ptr(t)
	}
	return res
}
