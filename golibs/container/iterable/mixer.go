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
package iterable

import (
	"fmt"
	"github.com/solarisdb/solaris/golibs"
	"github.com/solarisdb/solaris/golibs/errors"
)

type (
	// SelectF decides which one should be selected, it returns true if ev1
	// must be selected instead of ev2. If ev2 should be used then it
	// returns false
	SelectF[E any] func(ev1, ev2 E) bool

	// Mixer allows to mix 2 Iterators to one. Mixer provides the Iterator interface
	Mixer[E any] struct {
		sf         SelectF[E]
		src1, src2 srcDesc[E]
		st         byte
	}

	srcDesc[E any] struct {
		it   Iterator[E]
		load bool
		e    E
	}
)

var _ Iterator[int] = (*Mixer[int])(nil)

// Init initializes the mixer
func (mr *Mixer[E]) Init(sf SelectF[E], it1, it2 Iterator[E]) {
	mr.sf = sf
	mr.src1 = srcDesc[E]{it: it1, load: false, e: *new(E)}
	mr.src2 = srcDesc[E]{it: it2, load: false, e: *new(E)}
	mr.st = 0
}

// Reset allows to reset the mixer internals and retry underlying iterators.
// This function will also try to reset underlying iterators if they support
// Resetable interface. If the underlying iterators do not support Reset(),
// the Reset() will report ErrUnimplemented
func (mr *Mixer[E]) Reset() error {
	if err := mr.src1.reset(); err != nil {
		return err
	}
	if err := mr.src2.reset(); err != nil {
		return fmt.Errorf("cannot reset src2 %s: %w", err.Error(), errors.ErrDataLoss)
	}
	mr.st = 0
	return nil
}

// HasNext is the part of the Iterator interface
func (mr *Mixer[E]) HasNext() bool {
	mr.selectState()
	return mr.st != 3
}

// Next is the part of Iterator interface
func (mr *Mixer[E]) Next() (E, bool) {
	mr.selectState()
	switch mr.st {
	case 1:
		mr.st = 0
		mr.src1.load = false
		return mr.src1.e, true
	case 2:
		mr.st = 0
		mr.src2.load = false
		return mr.src2.e, true
	}
	return *new(E), false
}

func (mr *Mixer[E]) Close() error {
	err1 := mr.src1.close()
	err2 := mr.src2.close()
	mr.sf = nil
	if err1 == nil {
		return err2
	}
	return err1
}

func (mr *Mixer[E]) selectState() {
	if mr.st != 0 {
		return
	}
	if !mr.src1.load && mr.src1.it.HasNext() {
		mr.src1.e, mr.src1.load = mr.src1.it.Next()
	}

	if !mr.src2.load && mr.src2.it.HasNext() {
		mr.src2.e, mr.src2.load = mr.src2.it.Next()
	}

	if !mr.src1.load && !mr.src2.load {
		mr.st = 3
		return
	}

	if !mr.src1.load {
		mr.st = 2
		return
	}

	if !mr.src2.load || mr.testFunc() {
		mr.st = 1
		return
	}
	mr.st = 2
}

func (mr *Mixer[E]) testFunc() bool {
	return mr.sf(mr.src1.e, mr.src2.e)
}

func (sd *srcDesc[E]) close() error {
	var err error
	if sd.it != nil {
		err = sd.it.Close()
		sd.it = nil
		sd.e = *new(E)
	}
	return err
}

func (sd *srcDesc[E]) reset() error {
	sd.load = false
	sd.e = *new(E)
	rs, ok := sd.it.(golibs.Reseter)
	if ok {
		return rs.Reset()
	}
	return errors.ErrUnimplemented
}
