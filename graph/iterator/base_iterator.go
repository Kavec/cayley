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

// Defines an overridable and embeddable base iterator. This is similar to the
// Null iterator (in that it doesn't actually do anything), but provides the
// basic machinery for all iterators to build off of.

import (
	"github.com/google/cayley/graph"
)

type Base struct {
	uid      uint64
	tags     graph.Tagger
	result   graph.Value
	iterType graph.Type
	err      error
}

// After embedding the Base iterator struct, iterators
// must implement at least the following methods:
//   - Contains(graph.Value) bool
//   - Next()     bool
//   - Size()     (int64, bool)
//   - Optimize() (graph.iterator, bool)
//   - Stats()    graph.IteratorStats
//   - Clone()    graph.Iterator
//   - Reset()

// NewBase fills out the uid and iterType fields used by all iterators.
func NewBase(t graph.Type) Base {
	return Base{
		uid:      NextUID(),
		iterType: t,
	}
}

// Tagger returns the internal tags struct.
func (it *Base) Tagger() *graph.Tagger {
	return &it.tags
}

// Fill out the map based on tags assigned to this iterator.
func (it *Base) TagResults(dst map[string]graph.Value) {
	for _, tag := range it.Tagger().Tags() {
		dst[tag] = it.Result()
	}

	for tag, value := range it.Tagger().Fixed() {
		dst[tag] = value
	}
}

// Every iterator has a Result, most only use this function to report it.
func (it *Base) Result() graph.Value {
	return it.result
}

func (it *Base) UID() uint64 {
	return it.uid
}

func (it *Base) Err() error {
	return it.err
}

// Type returns whatever type Base was created with.
func (it *Base) Type() graph.Type {
	return it.iterType
}

func (it *Base) Describe() graph.Description {
	return graph.Description{
		UID:  it.UID(),
		Type: it.Type(),
		Tags: it.Tagger().Tags(),
	}
}

// Many iterators don't have a NextPath(), so that's been defined here.
func (it *Base) NextPath() bool {
	return false
}

// Subiterators have simple default behavior, too.
func (it *Base) SubIterators() []graph.Iterator {
	return nil
}

func (it *Base) Close() error {
	return nil
}
