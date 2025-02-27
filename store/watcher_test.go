package store

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestWatch(t *testing.T) {
	s := CreateStore(100)

	watchers := make([]*Watcher, 10)

	for i := range watchers {
		// create a new watcher
		watchers[i] = NewWatcher()
		// add to the watchers list
		s.AddWatcher("foo", watchers[i], 0)
	}

	s.Set("/foo/foo", "bar", time.Unix(0, 0), 1)

	for _, watcher := range watchers {
		// wait for the notification for any changing
		res := <-watcher.C
		if res == nil {
			t.Fatal("watcher is cleared")
		}
	}

	for i := range watchers {
		// create a new watcher
		watchers[i] = NewWatcher()
		// add to the watchers list
		s.AddWatcher("foo/foo/foo", watchers[i], 0)
	}

	s.watcher.stopWatchers()

	for _, watcher := range watchers {
		// wait for the notification for any changing
		res := <-watcher.C
		if res != nil {
			t.Fatal("watcher is cleared")
		}
	}
}

// 2025-02-27 21:14
// goos: linux
// goarch: amd64
// pkg: github.com/marsevilspirit/marstore/store
// cpu: 13th Gen Intel(R) Core(TM) i9-13980HX
// BenchmarkWatch-32    	    248	  4740162 ns/op
//
// BenchmarkWatch creates 10K watchers watch at /foo/[path] each time.
// Path is randomly chosen with max depth 10.
// It should take less than 15ms to wake up 10K watchers.
func BenchmarkWatch(b *testing.B) {
	s := CreateStore(100)

	key := make([]string, 10000)
	for i := range 10000 {

		key[i] = "/foo/"
		depth := rand.Intn(10)

		for range depth {
			key[i] += "/" + strconv.Itoa(rand.Int()%10)
		}
	}

	b.ResetTimer()
	for b.Loop() {
		watchers := make([]*Watcher, 10000)
		for i := range 10000 {
			// create a new watcher
			watchers[i] = NewWatcher()
			// add to the watchers list
			s.AddWatcher(key[i], watchers[i], 0)
		}

		s.watcher.stopWatchers()

		for _, watcher := range watchers {
			// wait for the notification for any changing
			<-watcher.C
		}

		s.watcher = newWatcherHub()
	}
}
