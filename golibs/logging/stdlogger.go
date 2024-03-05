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

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type (
	stdLogger struct {
		writer io.Writer
		name   string
		vars   map[string]string
	}
)

var (
	stdMx    sync.Mutex
	stdLevel int32 = int32(INFO)
	levels         = map[Level]string{ERROR: "ERROR", DEBUG: "DEBUG", INFO: "INFO", WARN: "WARN", TRACE: "TRACE"}
)

// stdNewLogger returns a Logger interface by its name
func stdNewLogger(name string) Logger {
	sl := new(stdLogger)
	sl.name = name
	sl.writer = os.Stdout
	sl.vars = map[string]string{}
	return sl
}

func stdSetLevel(lvl Level) {
	atomic.SwapInt32(&stdLevel, int32(lvl))
}

func stdGetLevel() Level {
	return Level(atomic.LoadInt32(&stdLevel))
}

// Warnf is a function for printing Warn-level messages from the source code
func (sl *stdLogger) Warnf(format string, args ...interface{}) {
	sl.logf(WARN, format, args...)
}

// Infof is a function for printing Info-level messages from the source code
func (sl *stdLogger) Infof(format string, args ...interface{}) {
	sl.logf(INFO, format, args...)
}

// Debugf is a function for printing Debug-level messages from the source code
func (sl *stdLogger) Debugf(format string, args ...interface{}) {
	sl.logf(DEBUG, format, args...)
}

// Tracef is a function for pretty printing Trace-level messages from the source code
func (sl *stdLogger) Tracef(format string, args ...interface{}) {
	sl.logf(TRACE, format, args...)
}

// Errorf is a function for pretty printing Error-level messages from the source code
func (sl *stdLogger) Errorf(format string, args ...interface{}) {
	sl.logf(ERROR, format, args...)
}

func (sl *stdLogger) logf(lvl Level, format string, args ...interface{}) {
	stdMx.Lock()
	if atomic.LoadInt32(&stdLevel) < int32(lvl) {
		stdMx.Unlock()
		return
	}
	now := time.Now()
	fmt.Fprint(sl.writer, "[", now.Format("15:04:05.000000"), "] ", levels[lvl], "\t", sl.name, ": ")
	fmt.Fprintf(sl.writer, format, args...)
	if len(sl.vars) > 0 {
		fmt.Fprintf(sl.writer, " %v", sl.vars)
	}
	fmt.Fprintln(sl.writer)
	stdMx.Unlock()
}
