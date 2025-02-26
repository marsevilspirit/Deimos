package store

import (
	"fmt"
	"testing"
	"time"
)

func TestStoreGet(t *testing.T) {
	Set("foo", "bar", time.Unix(0, 0))

	res := Get("foo")

	if res.Value != "bar" {
		t.Fatalf("Expected 'bar', got %s", res.Value)
	}

	Delete("foo")
	res = Get("foo")

	if res.Exist {
		t.Fatalf("Expected false, got %t", res.Exist)
	}
}

func TestExpire(t *testing.T) {
	fmt.Println(time.Now())
	fmt.Println("TEST EXPIRE")

	// test expire
	Set("foo", "bar", time.Now().Add(time.Second*1))
	time.Sleep(2 * time.Second)

	res := Get("foo")

	if res.Exist {
		t.Fatalf("Got expired value")
	}

	//test change expire time
	Set("foo", "bar", time.Now().Add(time.Second*10))

	res = Get("foo")

	if !res.Exist {
		t.Fatalf("Cannot get Value")
	}

	Set("foo", "barbar", time.Now().Add(time.Second*1))

	time.Sleep(2 * time.Second)

	res = Get("foo")

	if res.Exist {
		t.Fatalf("Got expired value")
	}

	// test change expire to stable
	Set("foo", "bar", time.Now().Add(time.Second*1))

	Set("foo", "bar", time.Unix(0, 0))

	time.Sleep(2 * time.Second)

	res = store.Get("foo")

	if !res.Exist {
		t.Fatalf("Cannot get Value")
	}

	// test stable to expire
	store.Set("foo", "bar", time.Now().Add(time.Second*1))
	time.Sleep(2 * time.Second)
	res = store.Get("foo")

	if res.Exist {
		t.Fatalf("Got expired value")
	}

	// test set older node
	store.Set("foo", "bar", time.Now().Add(-time.Second*1))
	res = store.Get("foo")

	if res.Exist {
		t.Fatalf("Got expired value")
	}

}
