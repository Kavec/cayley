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

// This package implements an adaptive, memory-bounded cache based on CART
// cf. Bansal, Sorav, and Dharmendra S. Modha.
//     "CAR: Clock with Adaptive Replacement." FAST. Vol. 4. 2004.
package cache

import (
	"container/list"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Defined for convenience/clarity
type emptySignal struct{}

type Materializer interface {
	// Materialize iteratively produces graph.Values and returns false when there
	// are either no more values or upon error
	Materialize() (graph.Value, bool)

	// Close informs the caching system that you're done with a materializer
	// and that memory may be freed-- materializers must not be used after Close()
	Close()

	// Error produces nil on successful operation or the latest valid error
	// Materializers must return nil again once all errors have been resolved
	Error() error
}

// The underlying caching data structure. For more information on the actual
// working machinery, please see runners.go (for cache runners) and request.go
type Cache struct {
	timeout   time.Duration
	sizeBytes int64

	// stable elements are recent ∪ frequent
	stable map[string]*list.Element
	// likewise, unstable = rHist ∪ fHist
	unstable map[string]*list.Element

	recent       *list.List
	frequent     *list.List
	rHist        *list.List
	fHist        *list.List
	sync.RWMutex // Only used during cache misses
}

type CacheOption func(*Cache) error

// New generates a new cache instance; each CacheOption will be applied
// to the new instance in turn
// cache instances default to a memory budget of 1GB and a timeout of 5min
func New(opts ...CacheOption) (*Cache, error) {
	sb, err := parseSizeBytes("1GB")
	if err != nil {
		panic(fmt.Sprintf(
			"parseSizeBytes returned error on initialization: %v",
			err.Error(),
		))
	}
	c := &cache{
		timeout:   5 * time.Minute,
		sizeBytes: sb,

		stable:   make(map[string]*list.Element),
		unstable: make(map[string]*list.Element),

		recent:   list.New(),
		frequent: list.New(),
		rHist:    list.New(),
		fHist:    list.New(),
	}

	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// ---- Cache Options ---- //

// Size accepts either a string or an int64 representing a cache's memory size
// - Size(n int64) sets the memory budget to n*bytes
// - Size(string) parses a size in B, KB, .., PB and converts to bytes
//     Matching is case-insentive, matches the whole string, and B is optional
//     Additionally, the numeric portion is passed to ParseFloat() prior to
//     computing the final size in bytes
func Size(ifce interface{}) CacheOption {
	return func(c *Cache) error {
		switch sz := ifce.(type) {
		case int64:
			c.sizeBytes = sz
			return nil
		case string:
			sb, err := parseSizeBytes(sz)
			if err != nil {
				return err
			}
			c.sizeBytes = sb
			return nil
		default:
			return fmt.Errorf("Unable to convert %T (%#v) into int64 (n_bytes)", sz, sz)
		}
	}
}

// Timeout tells the cache to toss stale requests-- this is set to 5*time.Minute
// by default, and a value of 0 will keep values around in the cache forever
func Timeout(t time.Duration) CacheOption {
	return func(c *Cache) error {
		c.timeout = t
		return nil
	}
}

// ---- User Operations ---- //

// Request is the main mechanism for working with the cache-- simply supply a
// query string and call the Materializer returned for each value in turn.
// something something describe behavior of immutable flag
//
// Materializers provided by c.Request() will replace their internals as needed
// to ensure all values generated are younger than the cache timout. While this
// may result in receiving a single value multiple times from a Materializer,
// Request guarantees each unique query string will be ran at most once per
// cache timeout window
func (c *Cache) Request(query string, immutable bool) Materializer {
	cr, hit := c.stable[query] //.(some casting)
	if hit && time.Since(cr.birth) < c.timeout {
		// Cache hits are super easy
		return cr.newMaterializer()
	}

	// Aaand now our misses-- need to do all the bookkeeping here
}

// -------------------------------- Internal -------------------------------- //

var sizexp = regexp.MustCompile(`^[0-9]*(?:\.[0-9]*)? ?([kmgtpKMGTP]?)[bB]?$`)

func parseSizeBytes(sz string) (int64, error) {
	m := sizexp.FindStringSubmatch(sz)
	if len(m) == 0 {
		return 0, fmt.Errorf("Unable to parse size string %v", sz)
	}
	bytes, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0, err
	}
	// switch here is being used like a JMP table-- go to highest size
	// and then multiply by 1024 each time you fallthrough to get final
	// size in bytes
	switch strings.ToLower(m[2]) {
	case `p`:
		bytes *= 1024
		fallthrough
	case `t`:
		bytes *= 1024
		fallthrough
	case `g`:
		bytes *= 1024
		fallthrough
	case `m`:
		bytes *= 1024
		fallthrough
	case `k`:
		bytes *= 1024
		fallthrough
	default:
		return int64(bytes), nil
	}
}

// cachedRequest represents a stream of data from the backend database,
// originally created at 'birth' and valid for the duration of the cache timeout
type cachedRequest struct {
	results []graph.Value
	dbView  Materializer
	birth   time.Time
	flags   uint8
}

func newCachedRequest(dbView Materializer) *CachedRequest {
	return &cachedRequest{
		results: make([]graph.Value),
		dbView:  dbView,
		birth:   time.Now(),
	}
}
