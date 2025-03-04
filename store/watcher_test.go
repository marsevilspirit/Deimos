package store

import (
	"testing"
)

func TestWatcher(t *testing.T) {
	s := newStore()
	wh := s.WatcherHub
	c, err := wh.watch("/foo", true, 1)

	if err != nil {
		t.Fatalf("%v", err)
	}

	select {
	case <-c:
		t.Fatal("should not receive from channel before send the event")
	default:
		// do nothing
	}

	e := newEvent(Create, "/foo/bar", 1, 1)

	wh.notify(e)

	re := <-c

	if e != re {
		t.Fatalf("recv != send")
	}

	c, _ = wh.watch("/foo", false, 2)

	e = newEvent(Create, "/foo/bar", 2, 2)

	wh.notify(e)

	select {
	case re = <-c:
		t.Fatal("should not receive from channel if not recursive", re)
	default:
		// do nothing
	}

	e = newEvent(Create, "/foo", 3, 3)

	wh.notify(e)

	re = <-c

	if e != re {
		t.Fatal("recv != send")
	}

	// ensure we are doing exact matching rather than prefix matching
	c, _ = wh.watch("/fo", true, 1)

	select {
	case re = <-c:
		t.Fatal("should not receive from channel:", re)
	default:
		// do nothing
	}

	e = newEvent(Create, "/fo/bar", 3, 3)

	wh.notify(e)

	re = <-c

	if e != re {
		t.Fatal("recv != send")
	}
}
