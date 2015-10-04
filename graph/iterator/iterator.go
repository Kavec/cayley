// Copyright 2014 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iterator

// Define the general iterator interface.

import (
	"sync/atomic"

	"github.com/google/cayley/graph"
)

var nextIteratorID uint64

func init() {
	atomic.StoreUint64(&nextIteratorID, 1)
}

func NextUID() uint64 {
	return atomic.AddUint64(&nextIteratorID, 1) - 1
}

// Here we define the simplest iterator -- the Null iterator. It contains nothing.
// It is the empty set. Often times, queries that contain one of these match nothing,
// so it's important to give it a special iterator.
type Null struct {
	Base
}

// Fairly useless New function.
func NewNull() *Null {
	return &Null{Base: NewBase(graph.Null)}
}

// The Null iterator never has results to tag.
func (it *Null) TagResults(dst map[string]graph.Value) {
	return
}

func (it *Null) Contains(graph.Value) bool {
	return false
}

func (it *Null) Clone() graph.Iterator { return NewNull() }

// A good iterator will close itself when it returns true.
// Null has nothing it needs to do.
func (it *Null) Optimize() (graph.Iterator, bool) { return it, false }

func (it *Null) Next() bool {
	return false
}

func (it *Null) Size() (int64, bool) {
	return 0, true
}

func (it *Null) Reset() {}

// Type is defined here as Null is often built with a struct literal to
// signal optimizing away an iterator.
func (it *Null) Type() graph.Type {
	return graph.Null
}

// A null iterator costs nothing. Use it!
func (it *Null) Stats() graph.IteratorStats {
	return graph.IteratorStats{}
}

var _ graph.Nexter = &Null{}
