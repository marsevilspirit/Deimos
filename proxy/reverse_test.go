package proxy

import (
	"bytes"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
)

type staticRoundTripper struct {
	res *http.Response
	err error
}

func (srt *staticRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return srt.res, srt.err
}

func TestReverseProxyServe(t *testing.T) {
	u := url.URL{Scheme: "http", Host: "192.0.2.3:4040"}

	tests := []struct {
		eps  []*endpoint
		rt   http.RoundTripper
		want int
	}{
		// no endpoints available so no requests are even made
		{
			eps: []*endpoint{},
			rt: &staticRoundTripper{
				res: &http.Response{
					StatusCode: http.StatusCreated,
					Body:       ioutil.NopCloser(&bytes.Reader{}),
				},
			},
			want: http.StatusServiceUnavailable,
		},

		// error is returned from one endpoint that should be available
		{
			eps:  []*endpoint{&endpoint{URL: u, Available: true}},
			rt:   &staticRoundTripper{err: errors.New("what a bad trip")},
			want: http.StatusBadGateway,
		},

		// endpoint is available and returns success
		{
			eps: []*endpoint{&endpoint{URL: u, Available: true}},
			rt: &staticRoundTripper{
				res: &http.Response{
					StatusCode: http.StatusCreated,
					Body:       ioutil.NopCloser(&bytes.Reader{}),
				},
			},
			want: http.StatusCreated,
		},
	}

	for i, tt := range tests {
		rp := reverseProxy{
			director:  &director{tt.eps},
			transport: tt.rt,
		}

		req, _ := http.NewRequest("GET", "http://192.0.2.2:4001", nil)
		rr := httptest.NewRecorder()
		rp.ServeHTTP(rr, req)

		if rr.Code != tt.want {
			t.Errorf("#%d: unexpected HTTP status code: want = %d, got = %d", i, tt.want, rr.Code)
		}
	}
}

func TestRedirectRequest(t *testing.T) {
	loc := url.URL{
		Scheme: "http",
		Host:   "bar.example.com",
	}

	req := &http.Request{
		Method: "GET",
		Host:   "foo.example.com",
		URL: &url.URL{
			Host: "foo.example.com",
			Path: "/v2/keys/baz",
		},
	}

	redirectRequest(req, loc)

	want := &http.Request{
		Method: "GET",
		// this field must not change
		Host: "foo.example.com",
		URL: &url.URL{
			// the Scheme field is updated to that of the provided URL
			Scheme: "http",
			// the Host field is updated to that of the provided URL
			Host: "bar.example.com",
			Path: "/v2/keys/baz",
		},
	}

	if !reflect.DeepEqual(want, req) {
		t.Fatalf("HTTP request does not match expected criteria: want=%#v got=%#v", want, req)
	}
}

func TestMaybeSetForwardedFor(t *testing.T) {
	tests := []struct {
		raddr  string
		fwdFor string
		want   string
	}{
		{"192.0.2.3:8002", "", "192.0.2.3"},
		{"192.0.2.3:8002", "192.0.2.2", "192.0.2.2, 192.0.2.3"},
		{"192.0.2.3:8002", "192.0.2.1, 192.0.2.2", "192.0.2.1, 192.0.2.2, 192.0.2.3"},
		{"example.com:8002", "", "example.com"},

		// While these cases look valid, golang net/http will not let it happen
		// The RemoteAddr field will always be a valid host:port
		{":8002", "", ""},
		{"192.0.2.3", "", ""},

		// blatantly invalid host w/o a port
		{"12", "", ""},
		{"12", "192.0.2.3", "192.0.2.3"},
	}

	for i, tt := range tests {
		req := &http.Request{
			RemoteAddr: tt.raddr,
			Header:     make(http.Header),
		}

		if tt.fwdFor != "" {
			req.Header.Set("X-Forwarded-For", tt.fwdFor)
		}

		maybeSetForwardedFor(req)
		got := req.Header.Get("X-Forwarded-For")
		if tt.want != got {
			t.Errorf("#%d: incorrect header: want = %q, got = %q", i, tt.want, got)
		}
	}
}

func TestRemoveSingleHopHeaders(t *testing.T) {
	hdr := http.Header(map[string][]string{
		// single-hop headers that should be removed
		"Connection":          []string{"close"},
		"Keep-Alive":          []string{"foo"},
		"Proxy-Authenticate":  []string{"Basic realm=example.com"},
		"Proxy-Authorization": []string{"foo"},
		"Te":                  []string{"deflate,gzip"},
		"Trailers":            []string{"ETag"},
		"Transfer-Encoding":   []string{"chunked"},
		"Upgrade":             []string{"WebSocket"},

		// headers that should persist
		"Accept": []string{"application/json"},
		"X-Foo":  []string{"Bar"},
	})

	removeSingleHopHeaders(&hdr)

	want := http.Header(map[string][]string{
		"Accept": []string{"application/json"},
		"X-Foo":  []string{"Bar"},
	})

	if !reflect.DeepEqual(want, hdr) {
		t.Fatalf("unexpected result: want = %#v, got = %#v", want, hdr)
	}
}

func TestCopyHeader(t *testing.T) {
	tests := []struct {
		src  http.Header
		dst  http.Header
		want http.Header
	}{
		{
			src: http.Header(map[string][]string{
				"Foo": []string{"bar", "baz"},
			}),
			dst: http.Header(map[string][]string{}),
			want: http.Header(map[string][]string{
				"Foo": []string{"bar", "baz"},
			}),
		},
		{
			src: http.Header(map[string][]string{
				"Foo":  []string{"bar"},
				"Ping": []string{"pong"},
			}),
			dst: http.Header(map[string][]string{}),
			want: http.Header(map[string][]string{
				"Foo":  []string{"bar"},
				"Ping": []string{"pong"},
			}),
		},
		{
			src: http.Header(map[string][]string{
				"Foo": []string{"bar", "baz"},
			}),
			dst: http.Header(map[string][]string{
				"Foo": []string{"qux"},
			}),
			want: http.Header(map[string][]string{
				"Foo": []string{"qux", "bar", "baz"},
			}),
		},
	}

	for i, tt := range tests {
		copyHeader(tt.dst, tt.src)
		if !reflect.DeepEqual(tt.dst, tt.want) {
			t.Errorf("#%d: unexpected headers: want = %v, got = %v", i, tt.want, tt.dst)
		}
	}
}
