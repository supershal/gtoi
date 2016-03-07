package gtoi

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kisielk/whisper-go/whisper"

	//"github.com/kisielk/whisper-go/whisper"
)

type Migration struct {
	debug              bool
	maxConcurrentFiles int // no of files to convert in parallel a a time.
}

func Migrate(config *Config, whisperDir string) error {
	// compile all regexp
	pc := config.PointConverters
	pc.compileRegex() // panic on invalid regex.

	//find all files under whisper dir.
	files, err := findWhisperFiles(whisperDir)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	errChan := make(chan error, 10)
	pointChans := make(chan chan InfluxPoint, len(files))

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
	wg.Wait()         // make sure that all pointChan (one per file) is added to channel of pointChan
	close(pointChans) // all pointChans are added. close channel of pointChan

	pChan := mergePointChans(pointChans)

	rChan := make(chan response, 10)
	go func() {
		config.InfluxClient.Send(pChan, rChan)
		close(rChan)
	}()
	for r := range rChan {
		fmt.Println("Response = ", r)
	}

	close(errChan)
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
