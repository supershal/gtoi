package gtoi

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

const backoffInterval = time.Duration(500 * time.Millisecond)

type InfluxClient struct {
	Enabled       bool     `toml:"enabled"`
	Addresses     []string `toml:"addresses"`
	Database      string   `toml:"database"`
	Precision     string   `toml:"precision"`
	BatchSize     int      `toml:"batch_size"`
	BatchInterval string   `toml:"batch_interval"`
	Concurrency   int      `toml:"concurrency"`

	r        chan<- response
	interval time.Duration
}

// Batch groups together points
func (c *InfluxClient) Send(ps <-chan InfluxPoint, r chan<- response) error {
	if !c.Enabled {
		return nil
	}

	c.r = r
	var buf bytes.Buffer
	var wg sync.WaitGroup
	counter := NewConcurrencyLimiter(c.Concurrency)

	interval, err := time.ParseDuration(c.BatchInterval)
	if err != nil {
		return err
	}
	c.interval = interval

	ctr := 0

	for p := range ps {
		b := p.Line()
		ctr++

		buf.Write(b)
		buf.Write([]byte("\n"))

		if ctr%c.BatchSize == 0 && ctr != 0 {
			b := buf.Bytes()

			// Trimming the trailing newline character
			b = b[0 : len(b)-1]

			wg.Add(1)
			counter.Increment()
			go func(byt []byte) {
				fmt.Println("Sending ", c.BatchSize, " Lines and Size ", len(byt), "bytes")
				c.fakeSendWithRetry(byt, time.Duration(1))
				counter.Decrement()
				wg.Done()
			}(b)

			var temp bytes.Buffer
			buf = temp
		}
	}
	// send remaining
	/////////////////////////////

	b := buf.Bytes()

	// Trimming the trailing newline character
	b = b[0 : len(b)-1]

	wg.Add(1)
	counter.Increment()
	go func(byt []byte) {
		fmt.Println("Sending ", ctr%c.BatchSize, " Lines and Size ", len(byt), "bytes")
		c.fakeSendWithRetry(byt, time.Duration(1))
		counter.Decrement()
		wg.Done()
	}(b)

	////////////////////////////

	wg.Wait()

	return nil
}

func (c *InfluxClient) fakeSendWithRetry(b []byte, backoff time.Duration) {
	d, _ := time.ParseDuration("1s")
	time.Sleep(d)
	rs := response{}
	c.r <- rs
}

// post sends a post request with a payload of points
func post(url string, datatype string, data io.Reader) (*http.Response, error) {
	resp, err := http.Post(url, datatype, data)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		err := errors.New(string(body))
		return nil, err
	}

	return resp, nil
}

// Send calls post and returns a response
func (c *InfluxClient) sendRequest(b []byte) (response, error) {

	t := time.Now()
	// TODO: fix c.Addresses to choose random interger from len(addresses)
	resp, err := post(c.Addresses[0], "application/x-www-form-urlencoded", bytes.NewBuffer(b))
	resDur := time.Now().Sub(t)
	if err != nil {
		return response{Duration: resDur}, err
	}

	r := response{
		Resp:     resp,
		Duration: resDur,
	}

	return r, nil
}

func (c *InfluxClient) sendWithRetry(b []byte, backoff time.Duration) {
	bo := backoff + backoffInterval
	rs, err := c.sendRequest(b)
	time.Sleep(c.interval)

	c.r <- rs
	if !rs.Success() || err != nil {
		time.Sleep(bo)
		c.sendWithRetry(b, bo)
	}
}
