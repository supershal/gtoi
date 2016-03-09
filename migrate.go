package gtoi

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/kisielk/whisper-go/whisper"

	//"github.com/kisielk/whisper-go/whisper"
)

type Migration struct {
	Debug              bool   `toml:"debug"`
	Interactive        bool   `toml:"interactive"`
	MaxConcurrentFiles int    `toml:"maxConcurrentFiles"`
	Database           string `toml:"database"`
	Host               string `toml:"host"`
	CreateDBAndRP      bool   `toml:"createDBAndRP"`
	Replication        int    `toml:"replication"`
	DefaultRP          string `toml:"defaultRP"`
	DefaultDuration    string `toml:"defaultDuration"`

	logger *log.Logger
}

func (m *Migration) SetLogger() {
	m.logger = log.New(os.Stderr, "[Migration] ", log.LstdFlags)
}

func (m *Migration) CreateDBIfNotExists() error {
	if !m.CreateDBAndRP {
		return nil
	}

	// create Http client
	cl, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: fmt.Sprintf("http://%v", m.Host),
	})
	if err != nil {
		return err
	}

	var input interface{}
	if m.Interactive {
		fmt.Printf("Create database \"%s\" if does not exists. \n Press any key to continue. Ctrl + c to abort: ", m.Database)
		fmt.Scanf("%v", &input)
	}

	// create database if not exists
	_, err = cl.Query(client.Query{
		Command: fmt.Sprintf("CREATE DATABASE %s", m.Database),
	})
	if err != nil {
		return err
	}
	m.logger.Println("Database ", m.Database, "created.")

	return nil
}

func (m *Migration) CreateRP(whisperDir string) ([]string, error) {

	var rpDays []string

	if !m.CreateDBAndRP {
		return rpDays, nil
	}

	cl, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: fmt.Sprintf("http://%v", m.Host),
	})
	if err != nil {
		return nil, err
	}

	// retrieve all retention policies
	rps, err := whisperRetentionPolicies(whisperDir)
	if err != nil {
		return nil, err
	}

	for _, d := range rps {
		rpDays = append(rpDays, toInfluxDbDuration(d))
	}

	var input interface{}
	if m.Interactive {
		fmt.Printf("Following retention policies will be created for database \"%v\": %v. \n Press any key to continue. Ctrl + c to abort: \n", m.Database, rpDays)
		fmt.Scanf("%v", &input)
	}

	// create retention policies

	for _, rpDay := range rpDays {
		_, err = cl.Query(client.Query{
			Command: fmt.Sprintf(" CREATE RETENTION POLICY \"%s\" ON \"%s\" DURATION %s REPLICATION %d", rpDay, m.Database, rpDay, m.Replication),
		})
		if err != nil {
			return nil, err
		}
	}
	m.logger.Println("Retention policies", rpDays, "created.")

	if m.DefaultRP != "" && m.DefaultDuration != "" {
		_, err = cl.Query(client.Query{
			Command: fmt.Sprintf("ALTER RETENTION POLICY \"%s\" ON \"%s\" DURATION %s REPLICATION %d DEFAULT", m.DefaultRP, m.Database, m.DefaultDuration, m.Replication),
		})
		if err != nil {
			return nil, err
		}
		m.logger.Println("Default Retention policy \"", m.DefaultRP, "\" created.")
	}

	return rpDays, nil
}

func whisperRetentionPolicies(whisperDir string) ([]time.Duration, error) {
	files, err := findWhisperFiles(whisperDir)
	if err != nil {
		return nil, err
	}
	var wg sync.WaitGroup
	rpChan := make(chan time.Duration)

	for _, f := range files {
		wg.Add(1)
		go func(file string) {
			defer wg.Done()
			w, err := whisper.Open(file)
			if err != nil {
				fmt.Printf("Cant open whisper file %v", err)
			}
			defer w.Close()

			for _, archive := range w.Header.Archives {
				ret := archive.Retention()
				rpChan <- time.Duration(ret) * time.Second
			}
		}(f)
	}

	go func() {
		wg.Wait()
		close(rpChan)
	}()

	rpMap := make(map[time.Duration]bool)
	for rp := range rpChan {
		rpMap[rp] = true
	}

	var rpSet []time.Duration
	for k, _ := range rpMap {
		rpSet = append(rpSet, k)
	}
	return rpSet, nil

}

func (m *Migration) Migrate(pc PointConverters, client *InfluxClient, whisperDir string) error {
	// compile all regexp
	pc.compileRegex() // panic on invalid regex.

	//find all files under whisper dir.
	files, err := findWhisperFiles(whisperDir)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	errChan := make(chan error, 10)
	pointChans := make(chan chan InfluxPoint, len(files)) //len must be equivalent to len of files. one point channel per file.

	for _, f := range files {
		wg.Add(1)

		// process each file in go routine and add the resulting channel to the point of channels
		go func(file string) {
			defer wg.Done()

			// find matching pattern from config
			gKey := filePathToPoint(file)
			wtoiStub, ok := pc.matchedConverter(gKey, errChan)
			if !ok {
				errChan <- fmt.Errorf("\nskipping file %v. No Pattern matched with graphite point %v \n", file, gKey)
				return
			}

			pointChan := migrateWspFile(file, wtoiStub, errChan)
			pointChans <- pointChan // add result pointChan to Channel of pointChan

		}(f)
	}

	go func() {
		wg.Wait()         // make sure that all pointChan (one per file) is added to channel of pointChan
		close(pointChans) // all pointChans are added. close channel of pointChan
	}()

	//Get merged channel which will have output from all the channels.
	pChan := mergePointChans(pointChans)

	// create HTTP response channel and send data to influxdb.
	rChan := make(chan Response, client.config.Concurrency)
	sendToInflux := func() {
		defer close(rChan)
		if err := client.Send(pChan, rChan, errChan); err != nil {
			errChan <- err
		}
	}
	go sendToInflux()

	// create summary object
	// TODO: create separate struct or handler function for summary
	var pointCount int
	var lat []float64
	for r := range rChan {
		pointCount += r.Pointcount
		lat = append(lat, float64(r.Duration.Nanoseconds()))
	}
	avgLat := average(lat)

	m.logger.Printf("Total points = %d and Avg latency = %v", pointCount, time.Duration(avgLat)*time.Nanosecond)

	close(errChan)
	for e := range errChan {
		m.logger.Println("Error: ", e)
	}

	return nil
}

func mergePointChans(pointChans chan chan InfluxPoint) <-chan InfluxPoint {
	var wg sync.WaitGroup
	out := make(chan InfluxPoint, 15000)

	output := func(in <-chan InfluxPoint) {
		for i := range in {
			out <- i
		}
		wg.Done()
	}

	for inChan := range pointChans {
		wg.Add(1)
		//fmt.Println("adding channel =", inChan)
		go output(inChan)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func migrateWspFile(file string, wtoiStub WspToInfStub, errChan chan<- error) chan InfluxPoint {
	pointChan := make(chan InfluxPoint, 15000) // for batch size of 10k
	go func(pointChan chan<- InfluxPoint) {
		defer close(pointChan)
		w, err := whisper.Open(file)
		if err != nil {
			errChan <- err
			return
		}
		defer w.Close()

		for i, archive := range w.Header.Archives {
			gPoints, err := w.DumpArchive(i)
			if err != nil {
				errChan <- err
				return
			}
			for _, gPoint := range gPoints {
				//fmt.Printf("%d: %d, %10.35g\n", n, point.Timestamp, point.Value)
				if gPoint.Timestamp == 0 {
					continue
				}

				gKey := filePathToPoint(file)
				gp := NewGraphitePoint(
					gKey,
					time.Duration(archive.Retention())*time.Second,
					gPoint.Value,
					int64(gPoint.Timestamp))
				pointChan <- wtoiStub.convert(gp)
			}
		}
	}(pointChan)

	return pointChan
}

func filePathToPoint(f string) string {
	f = strings.TrimSuffix(f, ".wsp")
	f = strings.Replace(f, "/", ".", -1)
	f = strings.Replace(f, " ", "_", -1)
	return f
}

func fakefindwhilsperFiles(searchDir string) ([]string, error) {
	return []string{
		"servers/emp000wo_us-west-2_e2e_4_apigee_com/GenericJMX-METRICS-PREVIOUS-inboundtraffic-ErrorResponsesSent_4XX/gauge-o$apigee-internal$e$prod$a$PublicAPI$r$2",
	}, nil
}

// Find all whisper files from a given wspPath
func findWhisperFiles(searchDir string) ([]string, error) {
	fileList := []string{}
	err := filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
		if os.IsNotExist(err) {
			return nil
		}
		if strings.HasSuffix(f.Name(), "wsp") {
			fileList = append(fileList, path)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return fileList, err
}

func average(s []float64) float64 {
	total := 0.0
	for _, v := range s {
		total += v
	}
	return total / float64(len(s))
}
