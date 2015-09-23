package cassandra

// (This notice has been ported and preserved for educational reasons)
//
// Na na na na na na na na na na na na na na na na... CACHE MA-
//
// *ahem*
// This file implements a cache manager that sits atop the LRU
// cache and coordinates reads/writes and DB Queries. Prior to
// cache man's crime-fighting spree, I was coordinating this with
// a per-value mutex that... well, cache man is an orphan for a reason
//
// Cache man is not the hero I deserve. I deserve far, far worse
//
// For a less tongue-in-cheek guide on when to use mutexes vs
// channels (AKA semaphores), please read:
// https://github.com/golang/go/wiki/MutexOrChannel

import (
	"fmt"
	"time"
)

// cacheMan is embedded right into the Cassandra struct, but defined here
// so as to be a ready reference
type cacheMan struct {
	db           *dbHandle
	cacheTimeout time.Duration
	checkPipes   chan *valPipe
	fillPipes    chan *valPipe

	cache *lru.Cache
}

// InitCache allocates channels for each part of the cache manager and
// then launches them in session-long goroutines
func (cm *cacheMan) InitCache() {
	// Capacity here is basically a random guess-- could really use metrics
	cm.checkPipes = make(chan *valPipe, 50)
	cm.fillPipes = make(chan *valPipe, 10)

	go cm.cacheCheck(cm.checkPipes)
	go cm.cacheFill(cm.fillPipes)
}

// TeardownCache closes pipes (allowing caching processes to terminate) and
// also purges the LRU cache itself
func (cm *cacheMan) TeardownCache() {
	close(cm.checkPipes)
	close(cm.fillPipes)
	cm.cache.Purge()
}

// ------------------------------------------ Internal ------------------------------------------ //

type lruEntry struct {
	birth time.Time
	data  interface{}
}

type valPipe struct {
	in  chan string // really <- chan, but we're closing it, too
	out chan<- interface{}
}

// Mostly provided for completeness
func (v *valPipe) Close() {
	close(v.in)
	close(v.out)
	v = nil
}

type inPipe chan<- string
type outPipe <-chan interface{}

func newPipes(vp chan *valPipe) (inPipe, outPipe) {
	in := make(chan string, 1)
	out := make(chan interface{}, 1)
	go func() {
		pipe := &valPipe{in, out}
		vp <- pipe
	}()
	return in, out
}

// cacheCheck is set up on init to feed incoming values to hit()
func (cm *cacheMan) cacheCheck(checkPipes chan *valPipe) {
	for pipe := range checkPipes {
		// Just here to delegate
		go cm.hit(pipe)
	}
}

// hit checks to see if we hit the cache-- if not, it hands off control
// to cacheFill() to make the actual DB request
func (cm *cacheMan) hit(pipe *valPipe) {
	rqst := <-pipe.in
	close(pipe.in)
	defer close(pipe.out)

	if rqst == "" {
		// Bad request, send a nil back out
		pipe.out <- nil
		return
	}

	if cm.cache.Contains(rqst) {
		ifce, _ := cm.cache.Get(rqst)
		if val := ifce.(*lruEntry); val != nil && time.Since(val.birth) < cm.cacheTimeout {
			pipe.out <- val.data
		}
		return
	}

	in, out := newPipes(cm.fillPipes)
	in <- rqst
	pipe.out <- <-out
}

// cacheFill coordinates requests against cassandra itself,
// ensuring that all requests in flight are unique. NB that it
// can be deadlocked if a value isn't immediately placed on its
// incoming channel, but only hit() should be placing values
// there (see a few lines above) and it should be doing that
// immediately.
func (cm *cacheMan) cacheFill(fillPipes chan *valPipe) {
	inFlight := map[string]bool{}
	waiting := map[string][]*valPipe{}
	flush := make(
		chan struct {
			rqst string
			resp interface{}
		}, 10)

	defer close(flush)
	for {
		select {
		case pipe, ok := <-fillPipes:
			if !ok {
				// Shutdown when fillPipes closes
				if pipe != nil {
					close(pipe.in)
					close(pipe.out)
				}
				return
			}

			rqst := <-pipe.in
			waiting[rqst] = append(waiting[rqst], pipe)
			close(pipe.in)
			if !inFlight[rqst] {
				inFlight[rqst] = true
				go cm.miss(rqst, flush)
			}

		// Once we receive values back from miss, pass them
		// back along to any consumers waiting for data; miss
		// handles dropping the values into the cache
		case f := <-flush:
			for _, pipe := range waiting[f.rqst] {
				go func(out chan<- interface{}) {
					out <- f.resp
					close(out)
				}(pipe.out)
			}

			delete(inFlight, f.rqst)
			delete(waiting, f.rqst)
		}
	}
}

// miss validates rqst and fires it off to the database, storing the
// data in the cache on response and pushing the data out via flush
//
// This function blocks on network I/O
func (cm *cacheMan) miss(
	rqst string,
	flush chan struct {
		rqst string
		resp interface{}
	},
) {

	resp := &lruEntry{}

	switch rqst {
	case "size":
		resp.data = db.fetchSize()
	case len(rqst) == 24: //nodeID
		resp.data = db.fetchNode(rqst)
	case len(rqst) == 24*3: //quadID
		resp.data = db.fetchQuad(rqst)
	default:
		panic(fmt.Sprintf("Unknown request type for %s", rqst))
	}

	resp.birth = time.Now()
	cd.cache.Add(rqst, resp)

	flush <- struct {
		rqst string
		resp interface{}
	}{rqst, val}
}
