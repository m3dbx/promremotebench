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
	"bytes"
	"fmt"
	"math/rand"
	"sync"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	"github.com/prometheus/prometheus/prompb"
)

const (
	separator = "_"
)

// Checker is used to read and write metrics to ensure accuracy.
type Checker interface {
	// Store stores values given a list of Prometheus timeseries.
	Store(map[string][]*prompb.TimeSeries)
	// Read returns the values stored.
	Read() map[string]ts.Datapoints
}

type checker struct {
	sync.RWMutex

	values map[string]ts.Datapoints
}

func newChecker() Checker {
	return &checker{
		values: make(map[string]ts.Datapoints),
	}
}

func (c *checker) Store(hostSeries map[string][]*prompb.TimeSeries) {
	for host, series := range hostSeries {
		for _, s := range series {
			dps := PromSamplesToM3Datapoints(s.Samples)
			// ts := []byte(strconv.FormatInt(sample.Timestamp, 10))
			// id = append(id, []byte(separator)...)
			// id = append(id, ts...)
			// idString := string(id)
			c.Lock()
			c.values[host] = dps
			// if _, ok := c.values[host]; ok {
			// 	c.values[host] += sample.Value
			// } else {
			// 	c.values[host] = sample.Value
			// }
			c.Unlock()
		}
	}

	if rand.Float64() < 0.5 {
		for name, val := range c.values {
			fmt.Println(name, ":", val)
		}
		fmt.Println("***********************")
	}
}

// PromSamplesToM3Datapoints converts Prometheus samples to M3 datapoints
func PromSamplesToM3Datapoints(samples []prompb.Sample) ts.Datapoints {
	datapoints := make(ts.Datapoints, 0, len(samples))
	tsMap := make(map[int64]float64)
	for _, sample := range samples {
		if _, ok := tsMap[sample.Timestamp]; ok {
			tsMap[sample.Timestamp] += sample.Value
		} else {
			tsMap[sample.Timestamp] = sample.Value
		}
	}

	for ts, val := range tsMap {
		timestamp := storage.PromTimestampToTime(ts)
		datapoints = append(datapoints, ts.Datapoint{Timestamp: timestamp, Value: val})
	}

	return datapoints
}

func (c *checker) Read() map[string]ts.Datapoints {
	return c.values
}

// The default name for the name and bucket tags in Prometheus metrics.
// This can be overwritten by setting tagOptions in the config.
var (
	promDefaultName       = []byte("__name__")
	promDefaultBucketName = []byte("le")
)

// PromLabelsToM3Tags converts Prometheus labels to M3 tags
func PromLabelsToM3Tags(
	labels []*prompb.Label,
	tagOptions models.TagOptions,
) models.Tags {
	tags := models.NewTags(len(labels), tagOptions)
	tagList := make([]models.Tag, 0, len(labels))
	for _, label := range labels {
		name := label.Name
		// If this label corresponds to the Prometheus name or bucket name,
		// instead set it as the given name tag from the config file.
		if bytes.Equal(promDefaultName, []byte(name)) {
			tags = tags.SetName([]byte(label.Value))
		} else if bytes.Equal(promDefaultBucketName, []byte(name)) {
			tags = tags.SetBucket([]byte(label.Value))
		} else {
			tagList = append(tagList, models.Tag{
				Name:  []byte(name),
				Value: []byte(label.Value),
			})
		}
	}

	return tags.AddTags(tagList)
}
