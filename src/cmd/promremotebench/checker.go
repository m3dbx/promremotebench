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
	Store(map[string][]prompb.TimeSeries)
	// GetDatapoints returns the values datapoints.
	GetDatapoints(hostname string) Datapoints
	// GetHostNames returns the active host names data is being
	// generated for.
	GetHostNames() []string
}

type checker struct {
	sync.RWMutex

	values  map[string]Datapoints
	aggFunc aggFunc
}

func newChecker(aggFunc aggFunc) Checker {
	return &checker{
		values:  make(map[string]Datapoints),
		aggFunc: aggFunc,
	}
}

func (c *checker) Store(hostSeries map[string][]prompb.TimeSeries) {
	for host, series := range hostSeries {
		if len(series) > 0 {
			dp := promSeriesToM3Datapoint(series, c.aggFunc)

			c.Lock()
			c.values[host] = append(c.values[host], dp)
			c.Unlock()
		}
	}
}

func (c *checker) GetDatapoints(hostname string) Datapoints {
	var dps Datapoints
	c.RLock()
	dps = c.values[hostname]
	c.RUnlock()

	return dps
}

func (c *checker) GetHostNames() []string {
	c.RLock()
	results := make([]string, len(c.values))
	i := 0
	for host, _ := range c.values {
		results[i] = host
		i++
	}
	c.RUnlock()

	return results
}

// promSeriesToM3Datapoint collapses Prometheus TimeSeries values to a single M3 datapoint
// with the aggregation function specified
func promSeriesToM3Datapoint(series []prompb.TimeSeries, aggFunc aggFunc) Datapoint {
	aggValue := 0.0
	var timestamp time.Time

	if len(series) > 0 && len(series[0].Samples) > 0 {
		timestamp = promTimestampToTime(series[0].Samples[0].Timestamp)
	}

	for _, s := range series {
		for _, sample := range s.Samples {
			aggValue = aggFunc(aggValue, sample.Value)
		}
	}

	return Datapoint{Timestamp: timestamp, Value: aggValue}
}

// promTimestampToTime converts a prometheus timestamp to time.Time.
func promTimestampToTime(timestampMS int64) time.Time {
	return time.Unix(0, timestampMS*int64(time.Millisecond))
}
