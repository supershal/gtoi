package gtoi

import (
	"bytes"
	"fmt"
	"strings"
	"time"
)

type WspToInf struct {
	PointRegex  string `toml:"pointRegex"`
	Measurement string `toml:"measurement"`
	Tags        Tags   `toml:"tag"`
	Field       Field  `toml:"field"`

	regex *Iregexp
}

type WspToInfStub WspToInf

func (w *WspToInf) compileRegex() {
	w.regex = NewIregexp(w.PointRegex)
}

func (w *WspToInf) match(s string) (WspToInfStub, error) {
	var stub WspToInfStub = WspToInfStub{}

	//validate regex
	if w.PointRegex == "" {
		return stub, fmt.Errorf("Value of pointRegexp is empty")
	}

	if !w.regex.Match(s) {
		return stub, fmt.Errorf("Regular expression \"%v\" does not match with \"%v\"", w.PointRegex, s)
	}

	kv := w.regex.FindStringSubMatchMap(s)
	// set measurment name.
	if stub.Measurement = w.expandMeasurement(kv); stub.Measurement == "" {
		return stub, fmt.Errorf("Value of measurement not found/matched for regex: \"%v\" \n point: \"%v\"", w.PointRegex, s)
	}

	var ok bool
	if stub.Tags, ok = w.expandTags(kv); !ok {
		return stub, fmt.Errorf("Value of one of the tag not found/matched for regex: \"%v\" \n point: \"%v\"", w.PointRegex, s)
	}

	if stub.Field, ok = w.expandFields(kv); !ok {
		return stub, fmt.Errorf("Value of one of the field not found/matched for regex: \"%v\" \n point: \"%v\"", w.PointRegex, s)
	}

	return stub, nil
}

func (w *WspToInf) expandTags(kv map[string]string) (Tags, bool) {
	var tags Tags
	for _, wt := range w.Tags {
		// no tag key or value should be empty
		if wt.Key == "" || wt.Value == "" {
			return tags, false
		}
		var tag Tag
		tag.Key = wt.Key
		if expVal := wt.Value[0]; expVal == '?' {
			tag.Value = kv[strings.Trim(wt.Value, "?")]
		} else {
			tag.Value = wt.Value
		}
		tags = append(tags, tag)
	}
	return tags, true
}

func (w *WspToInf) expandFields(kv map[string]string) (Field, bool) {
	var field Field

	wf := w.Field
	if wf.Key == "" {
		return field, false
	}

	if expVal := wf.Key[0]; expVal == '?' {
		field.Key = kv[strings.Trim(wf.Key, "?")]
	} else {
		field.Key = wf.Key
	}

	return field, true
}

func (w *WspToInf) expandMeasurement(kv map[string]string) string {
	if w.Measurement[0] != '?' {
		return w.Measurement
	}
	return kv[strings.Trim(w.Measurement, "?")]
}

func (stub *WspToInfStub) convert(gp *GraphitePoint) InfluxPoint {
	var inField = stub.Field
	inField.Value = gp.Value

	return *NewInfluxPoint(
		stub.Measurement,
		gp.Retention.String(),
		stub.Tags,
		inField,
		gp.Time)
}

type InfluxPoint struct {
	Measurement string
	RP          string
	Tags        Tags
	Field       Field // only one field required for graphite field "value"
	TimeStamp   int64
}

func NewInfluxPoint(measurement, rp string, tags Tags, field Field, epoch int64) *InfluxPoint {
	return &InfluxPoint{
		Measurement: measurement,
		RP:          rp,
		Tags:        tags,
		Field:       field,
		TimeStamp:   epoch,
	}
}

func (p InfluxPoint) Line() []byte {
	var buf bytes.Buffer

	buf.Write([]byte(fmt.Sprintf("%v,", p.Measurement)))
	buf.Write(p.Tags.toInfluxTagsLine())
	buf.Write([]byte(" "))
	buf.Write(p.Field.toInfluxFieldLine())
	buf.Write([]byte(" "))
	buf.Write([]byte(fmt.Sprintf("%v", p.TimeStamp)))

	byt := buf.Bytes()

	return byt
}

type GraphitePoint struct {
	Key       string
	Retention time.Duration
	Value     float64
	Time      int64
}

func NewGraphitePoint(key string, ret time.Duration, val float64, epoch int64) *GraphitePoint {
	return &GraphitePoint{
		Key:       key,
		Retention: ret,
		Value:     val,
		Time:      epoch,
	}
}
