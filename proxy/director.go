package proxy

import (
	"errors"
	"log"
	"net/url"
	"sync"
	"time"
)

const (
	// amount of time an endpoint will be held in a failed
	// state before being reconsidered for proxied requests
	endpointFailureWait = 5 * time.Second
)

// example addrs: "3=3.3.3.3&4=4.4.4.4&1=1.1.1.1&1=1.1.1.2&2=2.2.2.2"
func newDirector(scheme string, addrs []string) (*director, error) {
	if len(addrs) == 0 {
		return nil, errors.New("one or more upstream addresses required")
	}

	endpoints := make([]*endpoint, len(addrs))
	for i, addr := range addrs {
		u := url.URL{Scheme: scheme, Host: addr}
		endpoints[i] = newEndpoint(u)
	}

	d := director{ep: endpoints}
	return &d, nil
}

type director struct {
	ep []*endpoint
}

func (d *director) endpoints() []*endpoint {
	filtered := make([]*endpoint, 0)
	for _, ep := range d.ep {
		if ep.Available {
			filtered = append(filtered, ep)
		}
	}

	return filtered
}

func newEndpoint(u url.URL) *endpoint {
	ep := endpoint{
		URL:       u,
		Available: true,
		failFunc:  timedUnavailabilityFunc(endpointFailureWait),
	}

	return &ep
}

type endpoint struct {
	sync.Mutex

	URL       url.URL
	Available bool

	failFunc func(ep *endpoint)
}

func (ep *endpoint) Failed() {
	ep.Lock()
	if !ep.Available {
		ep.Unlock()
		return
	}

	ep.Available = false
	ep.Unlock()

	log.Printf("proxy: marked endpoint %s unavailable", ep.URL.String())

	if ep.failFunc == nil {
		log.Printf("proxy: no failFunc defined, endpoint %s will be unavailable forever.", ep.URL.String())
		return
	}

	ep.failFunc(ep)
}

func timedUnavailabilityFunc(wait time.Duration) func(*endpoint) {
	return func(ep *endpoint) {
		time.AfterFunc(wait, func() {
			ep.Available = true
			log.Printf("proxy: marked endpoint %s available", ep.URL.String())
		})
	}
}
