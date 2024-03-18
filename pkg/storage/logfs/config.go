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
	"github.com/solarisdb/solaris/golibs/files"
)

type Config struct {
	MaxRecordsLimit int
	MaxBunchSize    int
	// MaxLocks defines how many different logs may be managed at a time
	MaxLocks int
}

const (
	maxRecordsLimit = 10000
	maxBunchSize    = 2000 * files.BlockSize
)

func GetDefaultConfig() Config {
	return Config{
		MaxRecordsLimit: maxRecordsLimit,
		MaxBunchSize:    maxBunchSize,
		MaxLocks:        20000,
	}
}
