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

// Defines one of the base iterators, the All iterator. Which, logically
// enough, represents all nodes or all links in the graph.
//
// This particular file is actually vestigial. It's up to the QuadStore to give
// us an All iterator that represents all things in the graph. So this is
// really the All iterator for the memstore.QuadStore. That said, it *is* one of
// the base iterators, and it helps just to see it here.

import (
	"github.com/google/cayley/graph"
)

// An All iterator across a range of int64 values, from `max` to `min`.
type Int64 struct {
	Base

	max, min int64
	at       int64
	runstats graph.IteratorStats
}

// Creates a new Int64 with the given range.
func NewInt64(min, max int64) *Int64 {
	return &Int64{
		Base: NewBase(graph.All),
		min:  min,
		max:  max,
		at:   min,
	}
}

// Start back at the beginning
func (it *Int64) Reset() {
	it.at = it.min
}

func (it *Int64) Clone() graph.Iterator {
	out := NewInt64(it.min, it.max)
	out.tags.CopyFrom(it)
	return out
}

// Next() on an Int64 all iterator is a simple incrementing counter.
// Return the next integer, and mark it as the result.
func (it *Int64) Next() bool {
	graph.NextLogIn(it)
	it.runstats.Next += 1
	if it.at == -1 {
		return graph.NextLogOut(it, nil, false)
	}
	val := it.at
	it.at = it.at + 1
	if it.at > it.max {
		it.at = -1
	}
	it.result = val
	return graph.NextLogOut(it, val, true)
}

// Contains() for an Int64 is merely seeing if the passed value is
// withing the range, assuming the value is an int64.
func (it *Int64) Contains(tsv graph.Value) bool {
	graph.ContainsLogIn(it, tsv)
	it.runstats.Contains += 1
	v := tsv.(int64)
	if it.min <= v && v <= it.max {
		it.result = v
		return graph.ContainsLogOut(it, v, true)
	}
	return graph.ContainsLogOut(it, v, false)
}

// The number of elements in an Int64 is the size of the range.
// The size is exact.
func (it *Int64) Size() (int64, bool) {
	Size := ((it.max - it.min) + 1)
	return Size, true
}

// There's nothing to optimize about this little iterator.
func (it *Int64) Optimize() (graph.Iterator, bool) { return it, false }

// Stats for an Int64 are simple. Super cheap to do any operation,
// and as big as the range.
func (it *Int64) Stats() graph.IteratorStats {
	s, _ := it.Size()
	return graph.IteratorStats{
		ContainsCost: 1,
		NextCost:     1,
		Size:         s,
		Next:         it.runstats.Next,
		Contains:     it.runstats.Contains,
	}
}

var _ graph.Nexter = &Int64{}
