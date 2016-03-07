package gtoi

import (
	"bytes"
	"fmt"

	"github.com/BurntSushi/toml"
)

type Config struct {
	PointConverters PointConverters `toml:"pointconverters"`
	InfluxClient    InfluxClient    `toml:"influxclient"`
}

type PointConverters struct {
	WspToInfConvs []*WspToInf `toml:"whisper"`
}

func (p *PointConverters) compileRegex() {
	for _, w := range p.WspToInfConvs {
		w.compileRegex()
	}
}

func (p *PointConverters) matchedConverter(point string, errChan chan<- error) (WspToInfStub, bool) {
	for _, w := range p.WspToInfConvs {
		if stub, err := w.match(point); err == nil {
			return stub, true
		} else {
			errChan <- err
		}
	}
	return WspToInfStub{}, false
}

type Tag struct {
	Key   string `toml:"key"`
	Value string `toml:"value"`
}

type Tags []Tag

func (t Tags) toInfluxTagsLine() []byte {
	var buf bytes.Buffer
	for _, tag := range t {
		buf.Write([]byte(fmt.Sprintf("%v=%v,", tag.Key, tag.Value)))
	}

	b := buf.Bytes()
	b = b[0 : len(b)-1]
	return b
}

type Field struct {
	Key   string `toml:"key"`
	Value float64
}

func (f Field) toInfluxFieldLine() []byte {
	var buf bytes.Buffer

	buf.Write([]byte(fmt.Sprintf("%v=%v,", f.Key, f.Value)))

	b := buf.Bytes()
	b = b[0 : len(b)-1]

	return b
}

func DecodeConfig(file string) (*Config, error) {
	t := &Config{}

	if _, err := toml.DecodeFile(file, t); err != nil {
		return nil, err
	}

	return t, nil
}
