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
package config

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/solarisdb/solaris/golibs/errors"
)

// LoadJSONAndApply allows to load the key-values from the JSON file.
// The variables will be applied to the structure like ApplyEnvVariables function but without prefix.
// This function is useful for loading secrets file.
//
// Example:
// JSON file content:
// {"DB_PASSWORD": "123456"}
//
// The loader:
//
//	type DBConfig struct {
//		Password string
//	}
//
//	type Config struct {
//		DB DBConfig
//	}
//
// enricher := NewEnricher(Config{})
// LoadJSONAndApply(enricher, "/mnt/service/secrets")
// cfg := enricher.Value()
//
// The following Config fields will be set:
// cfg.DB.Password == "123456"
func LoadJSONAndApply[T any](e Enricher[T], path string) error {
	if path == "" {
		return fmt.Errorf("the function LoadEnvJSON() is called with empty path value: %w", errors.ErrInvalid)
	}
	buf, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("could not read file %s: %w", path, err)
	}
	keyValues := map[string]string{}
	err = json.Unmarshal(buf, &keyValues)
	if err != nil {
		return fmt.Errorf("could not unmarshal json file(%s): %w", path, err)
	}
	e.ApplyKeyValues("", "_", keyValues)
	return nil
}
