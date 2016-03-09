package gtoi

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

type InfluxClientConfig struct {
	Enabled       bool   `toml:"enabled"`
	Host          string `toml:"host"`
	Database      string `toml:"database"`
	Precision     string `toml:"precision"`
	BatchSize     int    `toml:"batch_size"`
	BatchInterval string `toml:"batch_interval"`
	Concurrency   int    `toml:"concurrency"`
	PreserveRP    bool   `toml:"preserveRP"`
}

type RPBatch struct {
	name   string
	batch  client.BatchPoints
	logger *log.Logger
}

func NewRPBatch(rpName string, config *InfluxClientConfig) RPBatch {

	b, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:         config.Database,
		Precision:        config.Precision,
		RetentionPolicy:  rpName,
		WriteConsistency: "1", // TODO: configurable.
	})
	return RPBatch{
		name:   rpName,
		batch:  b,
		logger: log.New(os.Stderr, "["+rpName+"-writer]", log.LstdFlags),
	}
}

func (rp *RPBatch) addPoint(p *client.Point) {
	rp.batch.AddPoint(p)
}

func (rp *RPBatch) write(address string) (Response, error) {
	if len(rp.batch.Points()) == 0 {
		return Response{}, nil
	}
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: fmt.Sprintf("http://%v", address),
	})
	if err != nil {
		return Response{}, err
	}
	defer c.Close()

	t1 := time.Now()
	err = c.Write(rp.batch)
	if err != nil {
		rp.logger.Println("error writing", err)
		return Response{}, err
	}
	lat := time.Now().Sub(t1)
	resp := Response{
		Database:   rp.batch.Database(),
		RP:         rp.batch.RetentionPolicy(),
		Pointcount: len(rp.batch.Points()),
		Duration:   lat,
	}

	rp.logger.Printf("%+v", resp)
	return resp, nil
}

type InfluxClient struct {
	config   *InfluxClientConfig
	interval time.Duration
	batches  map[string]RPBatch
}

func NewInfluxClient(c *InfluxClientConfig, policies ...string) *InfluxClient {
	bs := make(map[string]RPBatch)
	for _, rp := range policies {
		bs[rp] = NewRPBatch(rp, c)
	}
	return &InfluxClient{
		config:  c,
		batches: bs,
	}
}

func (c *InfluxClient) addPoint(p InfluxPoint) {
	rp := p.RP
	rpBatch := c.batches["default"]
	if nonDefault, ok := c.batches[rp]; ok {
		rpBatch = nonDefault
	}
	rpBatch.addPoint(p.Point())
}

// Batch groups together points
func (c *InfluxClient) Send(ps <-chan InfluxPoint, r chan<- Response, e chan<- error) error {
	if !c.config.Enabled {
		return nil
	}

	var wg sync.WaitGroup
	counter := NewConcurrencyLimiter(c.config.Concurrency)

	interval, err := time.ParseDuration(c.config.BatchInterval)
	if err != nil {
		return err
	}
	c.interval = interval

	ctr := 0

	for p := range ps {
		ctr++
		c.addPoint(p)

		if ctr%c.config.BatchSize == 0 && ctr != 0 {
			wg.Add(1)
			counter.Increment()
			go func(batches map[string]RPBatch) {
				defer wg.Done()
				for _, b := range batches {
					resp, err := b.write(c.config.Host)
					if err != nil {
						e <- err
					} else {
						r <- resp
					}
				}
				counter.Decrement()
			}(c.batches)
			c.flushBatches()
		}
	}
	// send remaining
	wg.Add(1)
	counter.Increment()
	go func(batches map[string]RPBatch) {
		defer wg.Done()
		for _, b := range batches {
			resp, err := b.write(c.config.Host)
			if err != nil {
				e <- err
			} else {
				r <- resp
			}
		}
		counter.Decrement()
	}(c.batches)
	c.flushBatches()
	wg.Wait()

	return nil
}

func (c *InfluxClient) flushBatches() {
	newBatchs := make(map[string]RPBatch)
	for rp, _ := range c.batches {
		newBatchs[rp] = NewRPBatch(rp, c.config)
	}

	c.batches = newBatchs
}
