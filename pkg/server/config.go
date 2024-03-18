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

package server

import (
	"encoding/json"
	"fmt"
	"github.com/solarisdb/solaris/golibs/config"
	"github.com/solarisdb/solaris/golibs/logging"
	"github.com/solarisdb/solaris/golibs/transport"
)

type (
	// Config defines the scaffolding-golang server configuration
	Config struct {
		// GrpcTransport specifies grpc transport configuration
		GrpcTransport *transport.Config
		// MetaDBFilePath specifies logs and chunks metadata is stored,
		// if left empty, in-memory storage is used
		MetaDBFilePath string
		// LocalDBFilePath specifies where the logs data is stored
		LocalDBFilePath string
		// MaxOpenedLogFiles allows to control number of files opened at a time to work with the solaris data
		// Increasing the number allows to increase the system performance for accessing to random group of logs
		MaxOpenedLogFiles int
	}
)

// getDefaultConfig returns the default server config
func getDefaultConfig() *Config {
	return &Config{
		GrpcTransport:     transport.GetDefaultGRPCConfig(),
		MetaDBFilePath:    ":memory:",
		LocalDBFilePath:   "slogs",
		MaxOpenedLogFiles: 100,
	}
}

func BuildConfig(cfgFile string) (*Config, error) {
	log := logging.NewLogger("solaris.ConfigBuilder")
	log.Infof("trying to build config. cfgFile=%s", cfgFile)
	e := config.NewEnricher(*getDefaultConfig())
	fe := config.NewEnricher(Config{})
	err := fe.LoadFromFile(cfgFile)
	if err != nil {
		return nil, fmt.Errorf("could not read data from the file %s: %w", cfgFile, err)
	}
	// overwrite default
	_ = e.ApplyOther(fe)
	_ = e.ApplyEnvVariables("SOLARIS", "_")
	cfg := e.Value()
	return &cfg, nil
}

// String implements fmt.Stringify interface in a pretty console form
func (c *Config) String() string {
	b, _ := json.MarshalIndent(*c, "", "  ")
	return string(b)
}
