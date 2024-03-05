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
package transport

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
)

// Config provides a network transport configuration
type Config struct {
	// Network defines the network for a network connection
	Network string // put "tcp" here

	// Address can have an interface for listening. Leave it empty to listen on all interfaces
	Address string

	// Port is the port the server will listen on
	Port int
}

// Addr returns the address string for the transport
func (c *Config) Addr() string {
	return fmt.Sprintf("%s:%d", c.Address, c.Port)
}

// ScanAddr turns a string in the format %s:%d into Config object. Network field will be empty
func ScanAddr(addr string) (Config, error) {
	var res Config
	idx := strings.LastIndex(addr, ":")
	if idx == -1 {
		res.Address = addr
		return res, nil
	}
	port, err := strconv.ParseInt(addr[idx+1:], 10, 32)
	if err != nil {
		err = fmt.Errorf("coudl not parse %s an integer port number: %w", addr[idx+1:], err)
	}
	res.Address = addr[:idx]
	res.Port = int(port)
	return res, err
}

// String implements fmt.Stringify
func (c *Config) String() string {
	b, _ := json.MarshalIndent(*c, "", "  ")
	return string(b)
}

// NewServerListener returns net.Listener by the config provided. We have the function separate just
// in case of we start to support different types of transports like TLS, the job will be done here.
func NewServerListener(cfg Config) (net.Listener, error) {
	return net.Listen(cfg.Network, cfg.Addr())
}

// GetDefaultGRPCConfig returns default GRPC config
func GetDefaultGRPCConfig() *Config {
	return &Config{
		Network: "tcp",
		Address: "",
		Port:    50051,
	}
}

// Apply assigns non-nil values from other to c
func (c *Config) Apply(other *Config) {
	if other == nil {
		return
	}
	if other.Network != "" {
		c.Network = other.Network
	}
	if other.Address != "" {
		c.Address = other.Address
	}
	if other.Port > 0 {
		c.Port = other.Port
	}
}
