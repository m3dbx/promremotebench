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
	"strconv"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/prometheus/prometheus/prompb"
)

const (
	separator = "_"
)

// Checker is used to read and write metrics to ensure accuracy.
type Checker interface {
	// Store stores values given a list of Prometheus timeseries.
	Store([]*prompb.TimeSeries)
	// Read returns the values stored.
	Read() map[string]float64
}

type checker struct {
	values map[string]float64
}

func newChecker() Checker {
	return &checker{
		values: make(map[string]float64),
	}
}

func (c *checker) Store(promSeries []*prompb.TimeSeries) {
	for _, series := range promSeries {
		tags := storage.PromLabelsToM3Tags(series.Labels, models.NewTagOptions())
		id := tags.ID()
		for _, sample := range series.Samples {
			ts := []byte(strconv.Itoa(sample.Timestamp))
			id = append(id, []byte(separator), ts)
			if ok := c.values[id]; ok {
				c.values[id] += sample.Value
			} else {
				c.values[id] = sample.Value
			}
		}
	}
}

func (c *checker) Read() map[string]float64 {
	return c.values
}
