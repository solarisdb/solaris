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
package logging

import "sync/atomic"

type (
	// Logger interface exposes some methods for application logging
	Logger interface {
		// Warnf is a function for printing Warn-level messages from the source code
		Warnf(format string, args ...interface{})
		// Infof is a function for printing Info-level messages from the source code
		Infof(format string, args ...interface{})
		// Debugf is a function for printing Debug-level messages from the source code
		Debugf(format string, args ...interface{})
		// Tracef is a function for pretty printing Trace-level messages from the source code
		Tracef(format string, args ...interface{})
		// Errorf is a function for pretty printing Error-level messages from the source code
		Errorf(format string, args ...interface{})
	}

	// Config struct allows to set the current logger settings
	Config struct {
		// NewLoggerF points to the function to construct new Logger
		NewLoggerF func(loggerName string) Logger
		// SetLevelF points to the function to set specific logger level
		SetLevelF func(lvl Level)
		// GetLevelF returns the current log level
		GetLevelF func() Level
	}

	// Level is one of ERROR, WARN, INFO, DEBUG, of TRACE
	Level int32
)

const (
	ERROR Level = iota
	WARN
	INFO
	DEBUG
	TRACE
)

var (
	loggerSettings atomic.Value
)

func init() {
	// init with the std logger
	SetConfig(Config{NewLoggerF: stdNewLogger, SetLevelF: stdSetLevel, GetLevelF: stdGetLevel})
}

// NewLogger returns the new instance of Logger for the caller name.
func NewLogger(loggerName string) Logger {
	return loggerSettings.Load().(Config).NewLoggerF(loggerName)
}

// SetLevel allows to set the logging level
func SetLevel(lvl Level) {
	loggerSettings.Load().(Config).SetLevelF(lvl)
}

// GetLevel returns the current log level
func GetLevel() Level {
	return loggerSettings.Load().(Config).GetLevelF()
}

// SetConfig allows to overwrite the current logger settings
func SetConfig(cfg Config) {
	loggerSettings.Store(cfg)
}
