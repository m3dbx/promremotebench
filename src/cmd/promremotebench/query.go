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
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

type queryExecutor struct {
	queryExecutorOptions
	client *http.Client
}

type queryExecutorOptions struct {
	URL           string
	Concurrency   int
	NumWriteHosts int
	NumSeries     int
	Step          time.Duration
	Range         time.Duration
	Aggregation   string
	Labels        map[string]string
	Headers       map[string]string
	Sleep         time.Duration
	Debug         bool
	DebugLength   int
	Logger        *zap.Logger
}

func newQueryExecutor(opts queryExecutorOptions) *queryExecutor {
	return &queryExecutor{
		queryExecutorOptions: opts,
		client:               http.DefaultClient,
	}
}

func (q *queryExecutor) Run() {
	q.Logger.Info("query load configured",
		zap.Int("concurrency", q.Concurrency))
	for i := 0; i < q.Concurrency; i++ {
		go q.workerLoop()
	}
}

func (q *queryExecutor) workerLoop() {
	// Select number of write hosts to select metrics from.
	numHosts := int(math.Ceil(float64(q.NumSeries) / 100.0))
	if numHosts < 1 {
		numHosts = 1
	}

	if numHosts > q.NumWriteHosts {
		q.Logger.Fatal("num series exceeds metrics emitted by write load num hosts",
			zap.Int("query-num-series", q.NumSeries),
			zap.Int("max-valid-query-num-series", q.NumWriteHosts*100),
			zap.Int("num-write-hosts", q.NumWriteHosts))
	}

	type label struct {
		name  string
		value string
	}
	labels := make([]label, 0, len(q.Labels))
	for k, v := range q.Labels {
		labels = append(labels, label{name: k, value: v})
	}

	pickedHosts := make(map[int]struct{})

	query := new(strings.Builder)
	for i := 0; ; i++ {
		func() {
			// Exec in func to be able to use defer.
			if i > 0 {
				time.Sleep(q.Sleep)
			}

			query.Reset()
			if q.Aggregation != "" {
				mustWriteString(query, q.Aggregation)
				mustWriteString(query, "({")
			}

			// Now we pick a few hosts to select metrics from, each should return 100 metrics.
			for k := range pickedHosts {
				delete(pickedHosts, k) // Reuse pickedHosts
			}
			mustWriteString(query, "hostname=~\"host_(")
			for j := 0; j < numHosts; j++ {
				hostNum := rand.Intn(q.NumWriteHosts)
				if _, ok := pickedHosts[hostNum]; ok {
					j-- // Try again
					continue
				}
				pickedHosts[hostNum] = struct{}{}
				mustWriteString(query, strconv.Itoa(hostNum))
				if j < numHosts-1 {
					mustWriteString(query, "|")
				}
			}
			mustWriteString(query, ")\"")

			// Write the common labels
			for j := 0; j < len(labels); j++ {
				l := labels[j]
				mustWriteString(query, l.name)
				mustWriteString(query, "=\"")
				mustWriteString(query, l.value)
				mustWriteString(query, "\"")
				if i < len(labels)-1 {
					mustWriteString(query, ",")
				}
			}

			if q.Aggregation != "" {
				mustWriteString(query, "})")
			}

			now := time.Now()
			values := make(url.Values)
			values.Set("query", query.String())
			values.Set("start", strconv.Itoa(int(now.Add(-1*q.Range).Unix())))
			values.Set("end", strconv.Itoa(int(now.Unix())))
			values.Set("step", q.Step.String())

			reqURL := fmt.Sprintf("%s?%s", q.URL, values.Encode())
			req, err := http.NewRequest(http.MethodGet, reqURL, nil)
			if err != nil {
				q.Logger.Error("create request error", zap.Error(err))
				return
			}

			if len(q.Headers) != 0 {
				for k, v := range q.Headers {
					req.Header.Set(k, v)
				}
			}

			if q.Debug {
				q.Logger.Info("execute query",
					zap.String("url", reqURL),
					zap.String("method", req.Method),
					zap.Any("values", values),
					zap.Any("headers", req.Header))
			}

			resp, err := q.client.Do(req)
			if err != nil {
				q.Logger.Error("failed to send request", zap.Error(err))
				return
			}

			defer func() {
				io.Copy(ioutil.Discard, resp.Body)
				resp.Body.Close()
			}()

			if resp.StatusCode/100 != 2 {
				q.Logger.Warn("response from query non-2XX status code",
					zap.Int("code", resp.StatusCode))
			}

			if q.Debug {
				reader := io.Reader(resp.Body)
				if q.DebugLength > 0 {
					reader = io.LimitReader(resp.Body, int64(q.DebugLength))
				}

				data, err := ioutil.ReadAll(reader)
				if err != nil {
					q.Logger.Error("failed to read response body",
						zap.Error(err))
				}

				q.Logger.Info("response body",
					zap.Int("limit", q.DebugLength),
					zap.ByteString("body", data))
			}
		}()
	}
}

func mustWriteString(w *strings.Builder, v string) {
	_, err := w.WriteString(v)
	if err != nil {
		panic(err)
	}
}
