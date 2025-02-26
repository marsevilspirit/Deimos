package store

import (
	"fmt"
	"testing"
	"time"
)

func TestWatch(t *testing.T) {
	// watcher := createWatcher()
	c := make(chan Response)
	d := make(chan Response)
	watcherHub.add("/", c)
	go say(c)
	watcherHub.add("/prefix/", d)
	go say(d)
	store.Set("/prefix/foo", "bar", time.Unix(0, 0))
}

func say(c chan Response) {
	result := <-c

	if result.Action != -1 {
		fmt.Println("yes")
	} else {
		fmt.Println("no")
	}
}
