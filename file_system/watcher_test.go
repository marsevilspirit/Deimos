package fileSystem

import (
	"testing"
)

func TestWatch(t *testing.T) {
	wh := newWatchHub(100)
	err, c := wh.watch("/foo", true, 0)

	if err != nil {
		t.Fatalf("%v", err)
	}

	select {
	case <-c:
		t.Fatal("should not receive from channel before send the event")
	default:
		// do nothing
	}

	e := newEvent(Set, "/foo/bar", 1, 0)

	wh.notify(e)

	re := <-c

	if e != re {
		t.Fatalf("recv != send")
	}

	_, c = wh.watch("/foo", false, 0)

	e = newEvent(Set, "/foo/bar", 1, 0)

	wh.notify(e)

	select {
	case <-c:
		t.Fatal("should not receive from channel if not recursive")
	default:
		// do nothing
	}

	e = newEvent(Set, "/foo", 1, 0)

	wh.notify(e)

	re = <-c

	if e != re {
		t.Fatal("recv != send")
	}
}
