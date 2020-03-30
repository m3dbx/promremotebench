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
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"promremotebench/pkg/generators"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestRemoteWrite(t *testing.T) {
	logger := instrument.NewOptions().Logger()
	tests := []struct {
		name            string
		numServers      int
		numHosts        int
		expectedBatches int
		expectedSeries  int
	}{
		{
			name:       "one host",
			numServers: 1,
			numHosts:   1,
		},
		{
			name:       "eleven hosts",
			numServers: 1,
			numHosts:   11,
		},
		{
			name:       "hundred hosts",
			numServers: 1,
			numHosts:   100,
		},
		{
			name:       "two servers one host",
			numServers: 2,
			numHosts:   1,
		},
		{
			name:       "two servers eleven hosts",
			numServers: 2,
			numHosts:   11,
		},
		{
			name:       "two servers hundred hosts",
			numServers: 2,
			numHosts:   100,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			numBatchesRecieved := 0
			numTSRecieved := 0
			serverUrls := make([]string, 0, test.numServers)
			var (
				wg sync.WaitGroup
				mu sync.Mutex
			)

			for i := 0; i < test.numServers; i++ {
				server := httptest.NewServer(
					http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						compressed, err := ioutil.ReadAll(r.Body)
						if err != nil {
							http.Error(w, err.Error(), http.StatusInternalServerError)
							return
						}

						reqBuf, err := snappy.Decode(nil, compressed)
						if err != nil {
							http.Error(w, err.Error(), http.StatusBadRequest)
							return
						}

						var req prompb.WriteRequest
						if err := proto.Unmarshal(reqBuf, &req); err != nil {
							http.Error(w, err.Error(), http.StatusBadRequest)
							return
						}

						mu.Lock()
						numBatchesRecieved++
						numTSRecieved += len(req.Timeseries)
						mu.Unlock()

						wg.Done()
					}),
				)

				serverURL, err := url.Parse(server.URL)
				if err != nil {
					t.Fatal(err)
				}

				serverUrls = append(serverUrls, serverURL.String())
			}

			fmt.Println(serverUrls)
			remotePromClient, err := NewClient(serverUrls, time.Minute, tally.NoopScope)
			if err != nil {
				t.Fatal(err)
			}

			hostGen := generators.NewHostsSimulator(test.numHosts, time.Now(),
				generators.HostsSimulatorOptions{})
			series, err := hostGen.Generate(time.Second, time.Second, 0)
			require.NoError(t, err)

			vals := []prompb.TimeSeries{}
			for _, s := range series {
				vals = append(vals, s...)
			}

			batchSize := 10
			expectedBatches := int(math.Ceil(float64(len(vals)) / float64(batchSize)))

			wg.Add(expectedBatches * test.numServers)
			remoteWrite(vals, remotePromClient, batchSize, nil, logger)
			wg.Wait()
			assert.Equal(t, expectedBatches*test.numServers, numBatchesRecieved)
			assert.Equal(t, len(vals)*test.numServers, numTSRecieved)

			fmt.Println("wrote series:", len(vals))
		})
	}
}
