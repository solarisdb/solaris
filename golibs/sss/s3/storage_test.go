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
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/solarisdb/solaris/golibs/sss"
	"testing"
)

// TODO set-up environment, otherwise run manually
// For example, to run the test you can use minio locally:
// docker run --rm -p 9000:9000 -p 9001:9001 -e "MINIO_ACCESS_KEY=username" -e "MINIO_SECRET_KEY=password" --name minio1  minio/minio server /data --console-address=:9001
func __TestS3General(t *testing.T) {
	s3c := &Storage{AwsConfig: &aws.Config{
		Credentials:      credentials.NewStaticCredentials("username", "password", ""),
		Endpoint:         aws.String("http://localhost:9000"),
		Region:           aws.String("us-west-1"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	},
		Bucket: "test",
	}
	s3c.Init(context.Background())
	sss.TestSimpleStorage(t, s3c)
}
