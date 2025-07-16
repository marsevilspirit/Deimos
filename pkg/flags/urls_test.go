package flags

import (
	"testing"
)

func TestValidateURLsBad(t *testing.T) {
	tests := []string{
		// bad IP specification
		":4001",
		"127.0:8080",
		"123:456",
		// bad port specification
		"127.0.0.1:foo",
		"127.0.0.1:",
		// unix sockets not supported
		"unix://",
		"unix://tmp/etcd.sock",
		// bad strings
		"somewhere",
		"234#$",
		"file://foo/bar",
		"http://hello/asdf",
	}
	for i, in := range tests {
		u := URLs{}
		if err := u.Set(in); err == nil {
			t.Errorf(`#%d: unexpected nil error for in=%q`, i, in)
		}
	}
}

func TestValidateURLsGood(t *testing.T) {
	tests := []string{
		"https://1.2.3.4:8080",
		"http://10.1.1.1:80",
		"http://10.1.1.1",
	}
	for i, in := range tests {
		u := URLs{}
		if err := u.Set(in); err != nil {
			t.Errorf("#%d: err=%v, want nil for in=%q", i, err, in)
		}
	}
}
