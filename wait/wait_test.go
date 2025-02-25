package wait

import (
	"fmt"
	"testing"
)

func TestWait(t *testing.T) {
	const id = 1
	wait := New()
	ch := wait.Register(id)
	wait.Trigger(id, "foo")
	v := <-ch
	if g, w := fmt.Sprintf("%v (%T)", v, v), "foo (string)"; g != w {
		t.Errorf("<-ch = %v, want %v", g, w)
	}

	if g := <-ch; g != nil {
		t.Errorf("unexpected non-nil value: %v (%T)", g, g)
	}
}

func TestRegisterDupSuppression(t *testing.T) {
	const id = 1
	wait := New()
	ch1 := wait.Register(id)
	ch2 := wait.Register(id)
	wait.Trigger(id, "foo")
	<-ch1
	if g := <-ch2; g != nil {
		t.Errorf("unexpected non-nil value: %v (%T)", g, g)
	}
}
