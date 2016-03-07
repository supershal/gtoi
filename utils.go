package gtoi

import (
	"net/http"
	"regexp"
	"time"
)

// iregexp is a wrapper for regexp.Regexp. It enhances functionalities of regexp.Regexp.
type Iregexp struct {
	*regexp.Regexp
}

func NewIregexp(s string) *Iregexp {
	return &Iregexp{regexp.MustCompile(s)}
}

func (r *Iregexp) Match(s string) bool {
	match := r.FindStringSubmatch(s)
	if match == nil {
		return false
	}
	return true
}

// FindStringSubMatchMap returns key value pairs extracted from a matching string.
// Example:
//    regex = "foo.(?P<varFoo>.+).bar(?P<varBar>.+)" and string = "foo.f.bar.b" reuturns
//    {"varFoo" : "f", "varBar": "b"}
func (r *Iregexp) FindStringSubMatchMap(s string) map[string]string {
	captures := make(map[string]string)

	match := r.FindStringSubmatch(s)
	if match == nil {
		return captures
	}

	for i, name := range r.SubexpNames() {
		if i == 0 {
			continue
		}

		captures[name] = match[i]
	}
	return captures
}

type response struct {
	Resp     *http.Response
	Duration time.Duration
}

// Success returns true if the request
// was successful and false otherwise.
func (r response) Success() bool {
	// ADD success for tcp, udp, etc
	return !(r.Resp == nil || r.Resp.StatusCode != 204)
}

//////////////////////////////////
// copied from influxdb/stress package

// ConcurrencyLimiter is a go routine safe struct that can be used to
// ensure that no more than a specifid max number of goroutines are
// executing.
type ConcurrencyLimiter struct {
	inc   chan chan struct{}
	dec   chan struct{}
	max   int
	count int
}

// NewConcurrencyLimiter returns a configured limiter that will
// ensure that calls to Increment will block if the max is hit.
func NewConcurrencyLimiter(max int) *ConcurrencyLimiter {
	c := &ConcurrencyLimiter{
		inc: make(chan chan struct{}),
		dec: make(chan struct{}, max),
		max: max,
	}
	go c.handleLimits()
	return c
}

// Increment will increase the count of running goroutines by 1.
// if the number is currently at the max, the call to Increment
// will block until another goroutine decrements.
func (c *ConcurrencyLimiter) Increment() {
	r := make(chan struct{})
	c.inc <- r
	<-r
}

// Decrement will reduce the count of running goroutines by 1
func (c *ConcurrencyLimiter) Decrement() {
	c.dec <- struct{}{}
}

// handleLimits runs in a goroutine to manage the count of
// running goroutines.
func (c *ConcurrencyLimiter) handleLimits() {
	for {
		r := <-c.inc
		if c.count >= c.max {
			<-c.dec
			c.count--
		}
		c.count++
		r <- struct{}{}
	}
}
