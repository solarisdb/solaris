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
package timeout

import (
	"container/heap"
	"fmt"
	"sync"
	"time"
)

type (
	// Future object allows to cancel a future execution request made by Call()
	Future interface {
		// Cancel allows to cancel a future execution.
		Cancel()
	}

	callControl struct {
		lock        sync.Mutex
		wakeCh      chan bool
		futures     *futures
		watchers    int
		idleTimeout time.Duration
		maxWorkers  int
	}

	future struct {
		cc    *callControl
		f     func()
		fireT time.Time
		idx   int
	}

	futures []*future

	dummyFuture struct{}
)

func init() {
	cc = newCallControl()
}

var cc *callControl

// VoidFuture maybe used to initialize a Future variable, without checking whether it is nil or not
var VoidFuture Future = dummyFuture{}

func newCallControl() *callControl {
	cc := new(callControl)
	cc.futures = &futures{}
	cc.maxWorkers = 10
	cc.wakeCh = make(chan bool, cc.maxWorkers)
	cc.idleTimeout = time.Second * 30
	heap.Init(cc.futures)
	return cc
}

// Call allows scheduling future execution of the function f in timeout provided.
// The function returns the Future object, which may be used for cancelling the execution
// request if needed.
func Call(f func(), timeout time.Duration) Future {
	return call(cc, f, timeout)
}

func call(cc *callControl, f func(), timeout time.Duration) Future {
	fu := new(future)
	fu.f = f
	fu.fireT = time.Now().Add(timeout)
	fu.idx = -1
	fu.cc = cc
	if f != nil {
		cc.add(fu)
	}
	return fu
}

// Cancel cancels the future execution if not called yet
func (fu *future) Cancel() {
	fu.cc.cancel(fu)
}

// String implements fmt.Stringify
func (fu *future) String() string {
	return fu.cc.futureAsString(fu)
}

func (cc *callControl) add(fu *future) {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	heap.Push(cc.futures, fu)
	if cc.watchers == 0 {
		cc.watchers++
		go cc.watcher()
	} else {
		cc.notifyWatcher()
	}
}

func (cc *callControl) futureAsString(fu *future) string {
	cc.lock.Lock()
	defer cc.lock.Unlock()
	f := "<not assigned>"
	if fu.f != nil {
		f = "<assigned>"
	}
	return fmt.Sprintf("{?f: %s, fireT: %v, changed: %t}", f, fu.fireT, fu.idx >= 0)
}

func (cc *callControl) cancel(fu *future) {
	cc.lock.Lock()
	defer cc.lock.Unlock()

	if fu.idx < 0 {
		return
	}
	fu.f = nil
	heap.Remove(cc.futures, fu.idx)
	if cc.watchers > 0 {
		cc.notifyWatcher()
	}
}

func (cc *callControl) notifyWatcher() {
	select {
	case cc.wakeCh <- true:
	default:
	}
}

func (cc *callControl) watcher() {
	misCount := 0
	var f func()
	for {
		if f != nil {
			f()
			f = nil
			misCount = 0
		} else {
			misCount++
		}

		var tmt time.Duration
		cc.lock.Lock()
		if cc.futures.Len() == 0 {
			if misCount > 1 {
				cc.watchers--
				cc.lock.Unlock()
				return
			}
			// if the worker did the job, let's sleep for the idle timeout and if no new jobs, let it go also
			tmt = cc.idleTimeout
		} else {
			fireT := (*cc.futures)[0].fireT
			now := time.Now()
			if now.After(fireT) {
				fu := heap.Pop(cc.futures).(*future)
				f = fu.f
				if cc.futures.Len() > 0 {
					fireT = (*cc.futures)[0].fireT
					if now.After(fireT) && cc.watchers < cc.maxWorkers {
						// spawn new worker if there is a job to do
						cc.watchers++
						go cc.watcher()
					}
				}
				cc.lock.Unlock()
				continue
			}

			tmt = fireT.Sub(now)
			if cc.watchers > 1 {
				// if the worker already slept once with no job, let it go
				if misCount > 1 {
					cc.watchers--
					cc.lock.Unlock()
					return
				}
				if tmt > cc.idleTimeout {
					tmt = cc.idleTimeout
				}
			}
		}
		cc.lock.Unlock()

		tmr := time.NewTimer(tmt)
		select {
		case <-tmr.C:
		case <-cc.wakeCh:
			if !tmr.Stop() {
				<-tmr.C
			}
			misCount = 0
		}
	}
}

func (fs *futures) Len() int {
	return len(*fs)
}

func (fs *futures) Less(i, j int) bool {
	fi := (*fs)[i]
	fj := (*fs)[j]
	return fi.fireT.Before(fj.fireT)
}

func (fs *futures) Swap(i, j int) {
	(*fs)[i], (*fs)[j] = (*fs)[j], (*fs)[i]
	(*fs)[i].idx, (*fs)[j].idx = i, j
}

func (fs *futures) Push(x any) {
	fu := x.(*future)
	fu.idx = fs.Len()
	(*fs) = append(*fs, fu)
}

func (fs *futures) Pop() any {
	last := fs.Len() - 1
	res := (*fs)[last]
	(*fs)[last] = nil
	(*fs) = (*fs)[:last]
	res.idx = -1
	return res
}

func (d dummyFuture) Cancel() {
	// Do nothing
}
