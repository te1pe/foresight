// Package queue moves scrape batches through Redpanda (Kafka API).
package queue

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/te1pe/foresight/internal/scrape"
)

// One Kafka record carries one scrape batch. JSON keeps the topic readable
// with "rpk topic consume" and tolerates added fields, which is all the
// schema evolution this project needs; a binary format would save bytes,
// but snappy compression of the record batches already removes most of
// JSON's redundancy.
type wireBatch struct {
	Target  string       `json:"target"`
	Time    time.Time    `json:"time"`
	Samples []wireSample `json:"samples"`
}

// Value is a string because JSON numbers cannot express NaN and ±Inf, which
// are legal in the exposition format (encoding/json refuses to marshal
// them). strconv's shortest round-trip formatting keeps it exact.
// Parsed exposition timestamps are not carried: storage uses Batch.Time.
type wireSample struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	Value  string            `json:"value"`
	Type   scrape.MetricType `json:"type"`
}

func Encode(b scrape.Batch) ([]byte, error) {
	w := wireBatch{Target: b.Target, Time: b.Time, Samples: make([]wireSample, len(b.Samples))}
	for i, s := range b.Samples {
		w.Samples[i] = wireSample{
			Name:   s.Name,
			Labels: s.Labels,
			Value:  strconv.FormatFloat(s.Value, 'g', -1, 64),
			Type:   s.Type,
		}
	}
	data, err := json.Marshal(w)
	if err != nil {
		return nil, fmt.Errorf("encode batch of %s: %w", b.Target, err)
	}
	return data, nil
}

func Decode(data []byte) (scrape.Batch, error) {
	var w wireBatch
	if err := json.Unmarshal(data, &w); err != nil {
		return scrape.Batch{}, fmt.Errorf("decode batch: %w", err)
	}
	if w.Target == "" || w.Time.IsZero() {
		return scrape.Batch{}, fmt.Errorf("decode batch: missing target or time")
	}

	b := scrape.Batch{Target: w.Target, Time: w.Time, Samples: make([]scrape.Sample, len(w.Samples))}
	for i, s := range w.Samples {
		v, err := strconv.ParseFloat(s.Value, 64)
		if err != nil {
			return scrape.Batch{}, fmt.Errorf("decode batch of %s: sample %s: invalid value %q", w.Target, s.Name, s.Value)
		}
		b.Samples[i] = scrape.Sample{Name: s.Name, Labels: s.Labels, Value: v, Type: s.Type}
	}
	return b, nil
}
