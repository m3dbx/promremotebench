// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package main

import (
	"sync"
	"time"

	"github.com/prometheus/prometheus/prompb"
)

const (
	separator = "_"
)

var (
	sumFunc = func(a, b float64) float64 {
		a += b
		return a
	}
)

type aggFunc func(float64, float64) float64

// A Datapoint is a single data value reported at a given time
type Datapoint struct {
	Timestamp time.Time
	Value     float64
}

// Datapoints is a list of datapoints.
type Datapoints []Datapoint

// Checker is used to read and write metrics to ensure accuracy.
type Checker interface {
	// Store stores values given a list of Prometheus timeseries.
	Store(map[string][]*prompb.TimeSeries)
	// Read returns the values stored.
	Read() map[string]Datapoints
}

type checker struct {
	sync.RWMutex

	values map[string]Datapoints
}

func newChecker() Checker {
	return &checker{
		values: make(map[string]Datapoints),
	}
}

func (c *checker) Store(hostSeries map[string][]*prompb.TimeSeries) {
	for host, series := range hostSeries {
		for _, s := range series {
			dps := promSamplesToM3Datapoints(s.Samples, sumFunc)
			c.Lock()
			c.values[host] = append(c.values[host], dps...)
			c.Unlock()
		}
	}
}

func (c *checker) Read() map[string]Datapoints {
	return c.values
}

// promSamplesToM3Datapoints converts Prometheus samples to M3 datapoints and aggregates
// them based on a given aggregation function.
func promSamplesToM3Datapoints(samples []prompb.Sample, aggFunc aggFunc) Datapoints {
	datapoints := make(Datapoints, 0, len(samples))
	aggValue := 0.0
	if len(samples) > 0 {
		timestamp := promTimestampToTime(samples[0].Timestamp)
		for _, sample := range samples {
			aggValue = aggFunc(aggValue, sample.Value)

		}
		datapoints = append(datapoints, Datapoint{Timestamp: timestamp, Value: aggValue})
	}

	return datapoints
}

// promTimestampToTime converts a prometheus timestamp to time.Time.
func promTimestampToTime(timestampMS int64) time.Time {
	return time.Unix(0, timestampMS*int64(time.Millisecond))
}
