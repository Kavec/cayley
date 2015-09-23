package cassandra

import (
	"github.com/gocql/gocql"
	"github.com/hashicorp/golang-lru"

	"github.com/google/cayley/graph"
)

const QuadstoreType = "cassandra"

type Cassandra struct {
	dbConfig     *gocql.ClusterConfig
	dbSession    *gocql.Session
	cacheTimeout time.Duration
	writeStyle   gocql.Consistency

	cacheMan

	query    chan empty
	nextTXID chan graph.PrimaryKey
}

type empty struct{}

func init() {
	graph.RegisterQuadStore(QuadStoreType, true, newCassandraStore, createNewCassandraGraph, nil)
}

func (cs *Cassandra) ApplyDeltas(deltas []graph.Delta, opts graph.IgnoreOpts) error {
	var wg sync.WaitGroup
	errCh := make(chan error, 5*len(deltas)) // link, s, p, o, b

	// Process through deltas to set off goroutines for writing
	for _, delta := range deltas {
		if !delta.Quad.Valid() {
			continue
		}
		switch delta.Action {
		case graph.Add:
			cs.writeDelta(&wg, errCh, delta.Quad, false) // deleted -> false
		case graph.Delete:
			cs.writeDelta(&wg, errCh, delta.Quad, true) // deleted -> true
		default:
			glog.Errorf("Unknown delta.Action of %#v", delta.Action)
		}
	}

	// Cache and log errors, returning the first one
	var err error
	go func() {
		for e := range errCh {
			if e != nil {
				if err != nil {
					err = e
				}
				glog.Errorf("Error while applying deltas: %s", err.Error())
			}
		}
	}()
	wg.Wait()
	close(errCh)

	// Invalidate size cache and force an update on next call now that
	// we've written changes to the backend
	cs.cache.Remove("size")

	return err
}

// Size returns the number of links in the database-- this function may
// block while fetching size if size count is older than cache_timeout
func (cs *Cassandra) Size() int64 {
	// Implemented internally; implementation detail isn't important here
	return cs.getSize()
}

// Horizon is more or less meaningless when using the Cassandra backend--
// we've offloaded all worries about consistency to Cassandra itself, since that's
// what the database is designed to handle. Instead of trying to second guess
// Cassandra, Horizon() essentially just counts number of transactions issued
func (cs *Cassandra) Horizon() graph.PrimaryKey {
	return <-nextTransactionID
}

// Closes and nils out the database handle to prevent reuse
func (cs *Cassandra) Close() {
	cs.dbSession.Close()
	cs.cache.Purge()

	close(nextTransactionID)
	cs = nil
}

// Type returns QuadstoreType ("cassandra")
func (cs *Cassandra) Type() string {
	return QuadstoreType
}

// ------------------------------------------ Internal ------------------------------------------ //

// writeDelta hands coordinating via a waitgroup, writes errors
// out via errCh, and writes all links/nodes from a delta
//
// WARNING: Cassandra batch operations don't work like other batching
// techniques-- to wit, batching node writes will SLOW operations.
// Please read the following for more information on when/where to
// batch writes (tl;dr- when maintaining multiple denormalized tables)
// http://docs.datastax.com/en/cql/3.1/cql/cql_using/useBatch.html
func (cs *Cassandra) writeDelta(
	wg *sync.WaitGroup, errCh chan error,
	link quad.Quad, deleted bool,
) {

	ids := []string{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cs.dbSession.
			Query(
			writeLink, strings.Join(ids, ""),
			ids[0], ids[1], ids[2],
			deleted,
		).
			Consistency(cs.writeStyle).
			Exec()

		var incDec string
		if !deleted {
			incDec = incrKeyCounts
		} else {
			incDec = decrKeyCounts
		}
		err = cs.dbSession.
			Query(incDec, "size").
			Consistency(cs.writeStyle).
			Exec()
		errCh <- err
	}()

	for dir := graph.Subject; dir <= graph.Label; dir++ {
		wg.Add(1)
		go func(d graph.Direction) {
			defer wg.Done()
			// graph.AnyDirection == 0, id starts at Subject
			errCh <- cs.writeNode(ids[d-1], link.Get(d), d, deleted)
		}(dir)
	}
}

func (cs *Cassandra) getSize() int64 {

}
