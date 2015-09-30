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

// This file implements machinery that converts a database request to a fully
// materialized view presented to clients
package cache

import (
	"sync"
	"time"

	"github.com/google/cayley/graph"
)

type request struct {
	// The cache we're a part of
	cache *Cache

	// Clients refer to elements in their view via index, so prior to doing
	// any work on this element (or clients themselves) ensure that all clients
	// have been told to halt reading
	view []*list.Element

	asks chan *askPipe

	// dbHost and clients lists are owned exclusively by the request struct
	// client materializers must not read from these fields
	dbHost  Materializer
	clients [*client]struct{}

	sync.RWMutex
}

type valueAsk struct {
	value graph.Value
	index int
	err   error
}

type askCh chan valueAsk
type ansCh chan valueAsk

// askPipe represents a pipe into the view management goroutine; calling code
// should put a valueAsk with an index into the in channel, then read off the
// filled out valueAsk from the out channel
type askPipe struct {
	in  askCh
	out ansCh
}

func (c *cache) newRequest(dbHost Materializer) *request {
	r := &request{
		cache: c,
		asks:  make(chan *askPipe),

		dbHost:  dbHost,
		clients: make(map[*clients]struct{}),
	}

	// kick off the process that manages a request's materialized view and
	// all client materializers
	go func() {
		var waiting []ansCh
		var err error
		materialized := make(chan graph.Value)

	Process:
		for {
			select {
			case pipe := <-r.asks:
				ask := <-pipe.in
				close(pipe.in)

				for i := ask.index; i < len(r.view); i++ {
					if r.view[i].Value.(cacheItem).immutable ||
						time.Since(r.view[i].Value.(cacheItem).birth) < r.cache.timeout {
						// Cache hit! Just fill out and send back
						ans := ask

						ans.index = i
						ans.value = r.view[i].Value.(cacheItem).value
						go func() { // Ensure we're not blocked
							// DO NOT add the error value to ans, since we're serving from
							// the local materialized view
							pipe.out <- ans
							close(pipe.out)
						}()

						continue Process
					}
				}
				// Aw dang, we're out of values here to hand out
				if waiting == nil {
					// Since there are no waiting requests, fire off a goroutine to
					// pull from our materializer while we continue to service other asks
					go func() {
						val, ok := r.dbHost.Materialize()
						if !ok {
							// Host materializer may have an error
							err = r.dbHost.Error()
						}
						materialized <- val
					}()
				}
				waiting = append(waiting, pipe.out)
			case val := <-materialized:
				// Alright, time to handle all the pending requests
				for _, ansCh := range waiting {
					go func() { // Don't let clients block us or each other
						ansCh <- &valueAsk{
							index: len(r.view),
							value: val,
							err:   err,
						}

						close(ansCh)
					}()
				}

				// List cleared, clear list
				waiting = nil
			}
		}
	}()

	return r
}

// askPipes generates a new askPipe-- each askPipe will accept one value via in
// and return one value via out (channels are closed after exactly one use)
func (r *request) askPipes() *askPipe {
	pipe := &askPipe{
		make(chan valueAsk),
		make(chan valueAsk),
	}

	r.asks <- pipe
	return pipe
}

// getValue provides an interface for client code to ask the request for
// the next value in their view while still allowing the request to maintain
// a single point of view-state mutation
func (r *request) getValue(clientIndex *int) (graph.Value, error) {
	if *clientIndex < 0 {
		*clientIndex = 0
	}

	pipe := r.askPipes()
	pipe.in <- &valueAsk{index: *clientIndex}
	ans := <-pipe.out

	*clientIndex = ans.index
	return ans.value, ans.err
}

func (r *request) removeClient(cl *client) {
	delete(r.clients, cl)
}

type client struct {
	host *request

	// index may be modified by the host, particularly during getValue and
	// when the host needs to sort its view. Take care to not modify this value
	// except in the actual goroutine managing this struct-- even then only when
	// the client materializer hasn't been asked to halt
	index int

	// readToggle is used to tell clients to halt when
	// the host request is about to modify their contents
	haltToggle chan signal
	values     chan graph.Value
	err        error
}

// newClientMaterializer generates a new materializer for client code to use
// instead of attempting to cache database calls themselves
func (r *request) newClientMaterializer() Materializer {
	cl := &client{
		host:       r,
		haltToggle: make(chan signal),
		values:     make(chan graph.Value),
	}

	go func() {
		for {
			select {
			case _, ok := <-haltToggle:
				if !ok {
					// This materializer has been closed
					return
				}
				// Block until we've been toggled back on
				_, ok = <-haltToggle
				if !ok {
					return // check if we've been closed while halting
				}
			default:
				// Need to take care to mutate all up front before being potentially
				// blocked by consumers of our materialized values

				cl.index += 1
				// host.getValue will adust or increment the index for us if cache items
				// at that index are too stale to be returned
				val, err := host.getValue(&cl.index)
				if err != nil {
					// Hit a snag-- save the error and close up shop
					cl.err = err
					close(cl.values)
				}

				// Now that we have a value and we've finished mutating our structure,
				// we're free to be blocked and wait for someone to read from our value
				// channel prior to reading or mutating structure again
				cl.values <- val
			}
		}
	}()
	return cl
}

func (cl *client) Materialize() {
	value, ok := <-cl.values
	return value, ok
}

func (cl *client) Close() {
	close(cl.values)
	close(cl.readToggle)

	host.removeClient(cl)
	cl = nil
	return
}

func (cl *client) Error() error {
	return cl.err
}
