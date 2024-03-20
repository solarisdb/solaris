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

package s3

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/solarisdb/solaris/golibs/errors"
	"github.com/solarisdb/solaris/golibs/sss"
	"io"
)

// Storage struct provides sss.Storage functionality on top of AWS S3
type Storage struct {
	AwsConfig *aws.Config `inject:""`
	Bucket    string      `inject:"AwsS3Bucket"`

	client *s3.S3
}

var _ sss.Storage = (*Storage)(nil)

// Init creates new S3 session to connect to S3
func (st *Storage) Init(_ context.Context) error {
	newSession, err := session.NewSession(st.AwsConfig)
	if err != nil {
		return fmt.Errorf("could not initialize Storage, bucket=%s: %w", st.Bucket, err)
	}
	st.client = s3.New(newSession)
	return nil
}

// Get receives value by its key
func (st *Storage) Get(key string) (io.ReadCloser, error) {
	if !sss.IsKeyValid(key) {
		return nil, fmt.Errorf("Storage.Get(): invalid key=%s: %w", key, errors.ErrInvalid)
	}

	res, err := st.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(st.Bucket),
		Key:    aws.String(toS3Path(key)),
	})
	if err != nil {
		return nil, toError(err)
	}

	return res.Body, nil
}

// Put allows to store value represented by reader r by the key
func (st *Storage) Put(key string, r io.Reader) error {
	if !sss.IsKeyValid(key) {
		return fmt.Errorf("Storage.Put(): invalid key=%s: %w", key, errors.ErrInvalid)
	}

	_, err := st.client.PutObject(&s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(r),
		Bucket: aws.String(st.Bucket),
		Key:    aws.String(toS3Path(key)),
	})
	if err != nil {
		return toError(err)
	}
	return nil
}

// List returns a list of keys and sub-paths (part of an existing path which
// is a path itself), which have the prefix of the path argument
func (st *Storage) List(path string) ([]string, error) {
	if !sss.IsPathValid(path) {
		return nil, fmt.Errorf("Storage.List(): path=%s is incorrect: %w", path, errors.ErrInvalid)
	}
	path = toS3Path(path)

	input := &s3.ListObjectsInput{
		Bucket:    aws.String(st.Bucket),
		Delimiter: aws.String("/"),
		Prefix:    aws.String(path),
		MaxKeys:   aws.Int64(100),
	}

	res := make([]string, 0, 10)
	for {
		result, err := st.client.ListObjects(input)
		if err != nil {
			return nil, toError(err)
		}

		for _, p := range result.CommonPrefixes {
			res = append(res, toKVSPath(aws.StringValue(p.Prefix)))
		}

		for _, c := range result.Contents {
			res = append(res, toKVSPath(aws.StringValue(c.Key)))
		}

		if !aws.BoolValue(result.IsTruncated) {
			break
		}
		input.Marker = result.NextMarker
	}
	return res, nil
}

// Delete allows to delete a value by key. Will return ErrNotFound if the key is not found
func (st *Storage) Delete(key string) error {
	if !sss.IsKeyValid(key) {
		return fmt.Errorf("Storage.Delete(): invalid key=%s: %w", key, errors.ErrInvalid)
	}

	_, err := st.client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(st.Bucket),
		Key:    aws.String(toS3Path(key)),
	})
	if err != nil {
		return toError(err)
	}
	return nil
}

func toS3Path(path string) string {
	return path[1:]
}

func toKVSPath(s3path string) string {
	return "/" + s3path
}

func toError(aerr error) error {
	if err, ok := aerr.(awserr.RequestFailure); ok {
		if err.StatusCode() == 404 {
			return errors.ErrNotExist
		}
	}
	return aerr
}
