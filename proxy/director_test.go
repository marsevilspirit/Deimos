package proxy

import (
	"net/url"
	"reflect"
	"testing"
)

func TestNewDirectorScheme(t *testing.T) {
	tests := []struct {
		scheme string
		addrs  []string
		want   []string
	}{
		{
			scheme: "http",
			addrs:  []string{"192.0.2.8:4002", "example.com:8080"},
			want:   []string{"http://192.0.2.8:4002", "http://example.com:8080"},
		},
		{
			scheme: "https",
			addrs:  []string{"192.0.2.8:4002", "example.com:8080"},
			want:   []string{"https://192.0.2.8:4002", "https://example.com:8080"},
		},

		// accept addrs without a port
		{
			scheme: "http",
			addrs:  []string{"192.0.2.8"},
			want:   []string{"http://192.0.2.8"},
		},

		// accept addrs even if they are garbage
		{
			scheme: "http",
			addrs:  []string{"."},
			want:   []string{"http://."},
		},
	}

	for i, tt := range tests {
		got, err := newDirector(tt.scheme, tt.addrs)
		if err != nil {
			t.Errorf("#%d: newDirectory returned unexpected error: %v", i, err)
		}

		for ii, wep := range tt.want {
			gep := got.ep[ii].URL.String()
			if !reflect.DeepEqual(wep, gep) {
				t.Errorf("#%d: want endpoints[%d] = %#v, got = %#v", i, ii, wep, gep)
			}
		}
	}
}

func TestDirectorEndpointsFiltering(t *testing.T) {
	d := director{
		ep: []*endpoint{
			{
				URL:       url.URL{Scheme: "http", Host: "192.0.2.5:5050"},
				Available: false,
			},
			{
				URL:       url.URL{Scheme: "http", Host: "192.0.2.4:4000"},
				Available: true,
			},
		},
	}

	got := d.endpoints()
	want := []*endpoint{
		{
			URL:       url.URL{Scheme: "http", Host: "192.0.2.4:4000"},
			Available: true,
		},
	}

	if !reflect.DeepEqual(want, got) {
		t.Fatalf("directed to incorrect endpoint: want = %#v, got = %#v", want, got)
	}
}
