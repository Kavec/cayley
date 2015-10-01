// Copyright 2015 The Cayley Authors. All rights reserved.
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

// A stream materializer is the basic data structure handed back to client code,
// representing all the values from a database request
package cache

import (
	"github.com/google/cayley/graph"
)

type stream struct {
	host *request

	// index may be modified by the host request, either when explicitly passed
	// via getValue or after the stream has been notified to halt processing
	index int

	reset  chan signal
	values chan graph.Value
	err    error

	closed bool
}

func (r *request) newStreamMaterializer() Materializer {
	st := &stream{
		host:   r,
		reset:  make(chan signal),
		values: make(chan graph.Value),
	}

	process := func() {
		defer close(values)
		for {
			select {
			case _, ok := <-reset:
				if !ok {
					// we've been closed
					return
				}
				st.index = 0
			default:
				st.index += 1

				val, ok := host.getValue(st.index)
				if !ok { // set error and stop producing values if bad times
					st.err = host.Error()
					return
				}
				st.values <- val // block here until we've been read
			}
		}
	}

	go process()
	return st
}

func (st *stream) Reset() {
	if st.closed {
		panic("Cannot reset closed stream")
	}
	st.reset <- signal
}

func (st *stream) Materialize() (graph.Value, bool) {
	if st.closed {
		panic("Cannot materialize from closed stream")
	}
	value, ok := <-st.values
	return value, ok
}

// Close closes and cleans up a stream; this function will always succeed--
// error here is the last error reported when generating stream values
//
// Closed streams must not be re-used except to read their error values
func (st *stream) Close() error {
	if st.closed {
		return st.err
	}
	st.closed = true
	close(st.reset)
	host.removeStream(st)
	return st.err
}

func (st *stream) Error() error {
	return st.err
}
