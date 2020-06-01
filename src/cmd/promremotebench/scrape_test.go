// Copyright (c) 2020 Uber Technologies, Inc.
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
	"testing"
	"time"

	"promremotebench/pkg/generators"

	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/devops"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

type testSimulator struct {
	series map[string][]prompb.TimeSeries
}

var _ generators.HostsSimulator = (*testSimulator)(nil)

func (*testSimulator) Hosts() []devops.Host {
	return []devops.Host{}
}

func (t *testSimulator) Generate(time.Duration, time.Duration, float64) (map[string][]prompb.TimeSeries, error) {
	return t.series, nil
}

type noopChecker struct {
}

var _ Checker = (*noopChecker)(nil)

func (*noopChecker) Store(map[string][]prompb.TimeSeries) {
}

func (*noopChecker) GetDatapoints(string) Datapoints {
	return Datapoints{}
}

func (*noopChecker) GetHostNames() []string {
	return []string{}
}

type label struct {
	name  string
	value string
}

func TestGatherSimple(t *testing.T) {
	series := map[string][]prompb.TimeSeries{
		"a": {prompb.TimeSeries{
			Labels:  []prompb.Label{{Name: "__name__", Value: "a"}, {Name: "foo", Value: "bar"}},
			Samples: []prompb.Sample{{Value: 0.9}},
		}},
	}
	simulator := &testSimulator{series}

	gatherer := newGatherer(simulator, time.Minute, 0, zaptest.NewLogger(t), &noopChecker{})

	result, err := gatherer.Gather()
	require.NoError(t, err)

	require.Equal(t, 1, len(result))

	assert.Equal(t, "a", *result[0].Name)
	assert.Equal(t, dto.MetricType_GAUGE, *result[0].Type)
	verifyGaugeValues(t, result[0].Metric, 0.9)
	verifyLabels(t, result[0].Metric, []label{{"foo", "bar"}})
}

func TestGatherComplex(t *testing.T) {
	seriesABar := prompb.TimeSeries{
		Labels:  []prompb.Label{{Name: "__name__", Value: "a"}, {Name: "foo", Value: "bar"}},
		Samples: []prompb.Sample{{Value: 0.5}, {Value: 0.6}},
	}
	seriesABaz := prompb.TimeSeries{
		Labels:  []prompb.Label{{Name: "__name__", Value: "a"}, {Name: "foo", Value: "baz"}},
		Samples: []prompb.Sample{{Value: 0.7}, {Value: 0.8}},
	}
	seriesBBar := prompb.TimeSeries{
		Labels:  []prompb.Label{{Name: "__name__", Value: "b"}, {Name: "foo", Value: "bar"}, {Name: "qux", Value: "baz"}},
		Samples: []prompb.Sample{{Value: 0.1}},
	}
	allSeries := map[string][]prompb.TimeSeries{
		"a": {seriesABar, seriesABaz},
		"b": {seriesBBar},
	}
	simulator := &testSimulator{allSeries}

	gatherer := newGatherer(simulator, time.Minute, 0, zaptest.NewLogger(t), &noopChecker{})

	result, err := gatherer.Gather()
	require.NoError(t, err)

	require.Equal(t, 2, len(result))

	if *result[0].Name == "b" { // ensure stable order
		tmp := result[1]
		result[1] = result[0]
		result[0] = tmp
	}

	assert.Equal(t, "a", *result[0].Name)
	assert.Equal(t, dto.MetricType_GAUGE, *result[0].Type)
	verifyGaugeValues(t, result[0].Metric, 0.5, 0.6, 0.7, 0.8)
	verifyLabels(t, result[0].Metric,
		[]label{{"foo", "bar"}},
		[]label{{"foo", "bar"}},
		[]label{{"foo", "baz"}},
		[]label{{"foo", "baz"}},
	)

	assert.Equal(t, "b", *result[1].Name)
	assert.Equal(t, dto.MetricType_GAUGE, *result[1].Type)
	verifyGaugeValues(t, result[1].Metric, 0.1)
	verifyLabels(t, result[1].Metric, []label{{"foo", "bar"}, {"qux", "baz"}})
}

func verifyGaugeValues(t *testing.T, metrics []*dto.Metric, expected ...float64) {
	require.Equal(t, len(expected), len(metrics))
	for i := range expected {
		assert.Equal(t, expected[i], *metrics[i].Gauge.Value)
	}
}

func verifyLabels(t *testing.T, metrics []*dto.Metric, expected ...[]label) {
	require.Equal(t, len(expected), len(metrics))
	for i := range expected {
		actualLabels := metrics[i].Label
		require.Equal(t, len(expected[i]), len(actualLabels))
		for j := range expected[i] {
			assert.Equal(t, expected[i][j].name, *actualLabels[j].Name)
			assert.Equal(t, expected[i][j].value, *actualLabels[j].Value)
		}
	}
}
