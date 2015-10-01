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

// This file implements cached database requests-- each request maintains stream
// materializers, and the results they represent, resetting each one onc
package cache

import (
	"time"

	"github.com/google/cayley/graph"
)

type request struct {
	host *Cache

	immutable bool
	birth     time.Time

	view []*list.Element

	asks chan *askPipe

	dbStream Materializer
	streams  [*stream]struct{}

	err error
}

type valueAsk struct {
	index int
	value graph.Value
	err   error
}

type askCh chan valueAsk
type ansCh chan valueAsk

type askPipe struct {
	in  askCh
	out ansCh
}

func (c *cache) newRequest(dbStream Materializer) *request {
	r := &request{
		cache:    c,
		asks:     make(chan *askPipe),
		dbStream: dbStream,
		streams:  make(map[*stream]struct{}),
	}

	process := func() {
		var waiting []ansCh
		defer func() {
			for _, ch := range waiting {
				close(ch)
			}
		}()

		materialized := make(chan graph.Value)
		for {
			select {
			case pipe, ok := <-r.asks:
				if !ok {
					return
				}
				ask := <-pipe.in
				close(pipe.in)

				if ask.index < len(r.view) {
					var ans *valueAsk = ask // copy pointer for clarity

					// No error, just serving a cached item
					ans.value = r.view[ask.index].Value.(cacheItem).value
					go func() { // Ensure we're never blocked from serving requests
						pipe.out <- ans
						close(pipe.out)
					}()

					continue
				}
				// cache miss
				if waiting == nil { // fire off potentially long netio op
					go func() {
						val, ok := r.dbStream.Materialize()
						if !ok {
							r.err = r.dbStream.Error()
						}
						materialized <- val
					}()
				}
				waiting = append(waiting, pipe.out)
			case val := <-materialized:
				for _, ansCh := range waiting {
					go func() { // Don't let clients block us or each other
						ansCh <- &valueAsk{
							value: val,
							err:   r.err,
						}

						close(ansCh)
					}()
				}

				// waiting list cleared, clear waiting list
				waiting = nil
			}
		}
	}

	go process()
	return r
}

// askPipes provides a new one-time-use pair of channels into and out of
// the request materializer multiplexer. Both channels will be closed
// by the process defined at launch of the request
func (r *request) askPipes() *askPipe {
	pipe := &askPipe{
		make(chan valueAsk),
		make(chan valueAsk),
	}

	r.asks <- pipe
	return pipe
}

// getValue, unfortunately, makes a request a special case away from being
// a materializer itself-- the index argument is used so clients may identify
// which value they need to have materialized
//
// I'm unsure if materializers are worth generalizing until request becomes one
func (r *request) getValue(index int) (graph.Value, bool) {
	if index < 0 { // simple sanity check here
		index = 0 // index >= len(r.views) handled elsewhere
	}

	pipe := r.askPipes()
	pipe.in <- &valueAsk{index: index}
	ans := <-pipe.out

	if ans != nil {
		// ans.err is used here because while the request may have an error
		// not all values in the materialized view have an error--
		// client code shouldn't worry about errors in the stream until they've
		// read off all the values from their local materializer
		return ans.value, (ans.err == nil)
	}

	// And here we may have an error clients should care about
	return nil, (r.err == nil)
}

func (r *request) removeStream(st *stream) {
	delete(r.streams, st)
}

// Reset the db Materializer and all stream Materializers
func (r *request) Reset() {
	r.dbStream.Reset()
	for _, s := range r.streams {
		s.Reset()
	}
}

func (r *request) Close() error {
	if !r.closed {
		r.dbStream.Close()
		for _, s := range r.streams {
			s.Close()
		}
		close(r.asks)
	}
	return r.err
}

func (r *request) Error() error {
	return r.err
}
