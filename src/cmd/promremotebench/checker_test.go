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
	"testing"
	"time"

	"promremotebench/pkg/generators"

	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
)

func TestChecker(t *testing.T) {
	checker := newChecker(checkerOptions{
		aggFunc:               sumFunc,
		cleanTickDuration:     time.Minute,
		expiredSeriesDuration: time.Minute,
		targetLen:             10,
	}, 10)
	hostGen := generators.NewHostsSimulator(10, time.Now(),
		generators.HostsSimulatorOptions{})
	series, err := hostGen.Generate(time.Second, time.Second, 0)
	require.NoError(t, err)

	checker.Store(series)
	hostnames := checker.GetHostNames()
	require.Equal(t, 10, len(hostnames))

	for _, hostname := range hostnames {
		dps := checker.GetDatapoints(hostname)
		require.Equal(t, 1, len(dps))
	}
}

func TestCheckerCleanup(t *testing.T) {
	checker := newChecker(checkerOptions{
		aggFunc:               sumFunc,
		cleanTickDuration:     15 * time.Millisecond,
		expiredSeriesDuration: 20 * time.Millisecond,
		targetLen:             2,
	}, 2)

	firstUnixMilliseconds := time.Now().Add(-30*time.Millisecond).UnixNano() / int64(time.Millisecond)

	sample := prompb.Sample{
		Value:     1.0,
		Timestamp: firstUnixMilliseconds,
	}
	series1 := prompb.TimeSeries{
		Samples: []prompb.Sample{sample},
	}
	series2 := prompb.TimeSeries{
		Samples: []prompb.Sample{sample},
	}
	hostMap := map[string][]prompb.TimeSeries{
		"host1": []prompb.TimeSeries{series1},
		"host2": []prompb.TimeSeries{series2},
	}

	checker.Store(hostMap)
	hostnames := checker.GetHostNames()
	require.Equal(t, 2, len(hostnames))

	secondUnixMilliseconds := time.Now().Add(-10*time.Millisecond).UnixNano() / int64(time.Millisecond)
	series1.Samples[0].Timestamp = secondUnixMilliseconds
	checker.Store(map[string][]prompb.TimeSeries{
		"host1": []prompb.TimeSeries{series1},
	})

	lastUnixMilliseconds := time.Now().UnixNano() / int64(time.Millisecond)
	series1.Samples[0].Timestamp = lastUnixMilliseconds
	checker.Store(map[string][]prompb.TimeSeries{
		"host1": []prompb.TimeSeries{series1},
	})

	dps := checker.GetDatapoints("host1")
	require.Equal(t, 3, len(dps))

	for len(checker.GetHostNames()) == 2 {
		time.Sleep(5 * time.Millisecond)
	}

	require.Equal(t, 1, len(checker.GetHostNames()))
	dps = checker.GetDatapoints("host1")
	require.Equal(t, 2, len(dps))
	require.Equal(t, promTimestampToTime(secondUnixMilliseconds), dps[0].Timestamp)
	require.Equal(t, promTimestampToTime(lastUnixMilliseconds), dps[1].Timestamp)
}
