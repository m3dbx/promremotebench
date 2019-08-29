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
	"encoding/json"
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

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/stats"

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

func (q *queryExecutor) Run(checker Checker) {
	q.Logger.Info("query load configured",
		zap.Int("concurrency", q.Concurrency))
	for i := 0; i < q.Concurrency; i++ {
		go q.alertLoad(checker)
	}

	go q.accuracyCheck(checker)
}

// accuracyCheck checks the accuracy of data for one
// host at a time.
func (q *queryExecutor) accuracyCheck(checker Checker) {
	type label struct {
		name  string
		value string
	}
	labels := make([]label, 0, len(q.Labels))
	for k, v := range q.Labels {
		labels = append(labels, label{name: k, value: v})
	}

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

			curHostnames := checker.GetHostNames()
			if len(curHostnames) == 0 {
				q.Logger.Error("no hosts returned in the checker, skipping accuracy check")
				return
			}

			selectedHost := curHostnames[rand.Intn(len(curHostnames))]
			mustWriteString(query, "hostname=\""+selectedHost+"\"")

			// Write the common labels.
			for j := 0; j < len(labels); j++ {
				mustWriteString(query, ",")

				l := labels[j]
				mustWriteString(query, l.name)
				mustWriteString(query, "=\"")
				mustWriteString(query, l.value)
				mustWriteString(query, "\"")
			}

			if q.Aggregation != "" {
				mustWriteString(query, "})")
			}

			res := q.executeQuery(query, true)
			if len(res) == 0 {
				q.Logger.Error("invalid response for accuracy query")
			}

			q.validateQuery(checker.GetDatapoints(selectedHost), res)
		}()
	}
}

func (q *queryExecutor) alertLoad(checker Checker) {
	// Select number of write hosts to select metrics from.
	numHosts := int(math.Ceil(float64(q.NumSeries) / 101.0))
	if numHosts < 1 {
		numHosts = 1
	}

	if numHosts > q.NumWriteHosts {
		q.Logger.Fatal("num series exceeds metrics emitted by write load num hosts",
			zap.Int("query-num-series", q.NumSeries),
			zap.Int("max-valid-query-num-series", q.NumWriteHosts*101),
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

	pickedHosts := make(map[string]struct{})

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

			curHostnames := checker.GetHostNames()
			if len(curHostnames) == 0 {
				q.Logger.Error("no hosts returned in the checker, skipping load test round")
				return
			}

			// Now we pick a few hosts to select metrics from, each should return 101 metrics.
			for k := range pickedHosts {
				delete(pickedHosts, k) // Reuse pickedHosts
			}
			mustWriteString(query, "hostname=~\"(")
			for j := 0; j < numHosts; j++ {
				hostIndex := rand.Intn(len(curHostnames))
				if _, ok := pickedHosts[curHostnames[hostIndex]]; ok {
					j-- // Try again.
					continue
				}
				pickedHosts[curHostnames[hostIndex]] = struct{}{}
				mustWriteString(query, curHostnames[hostIndex])
				if j < numHosts-1 {
					mustWriteString(query, "|")
				}
			}
			mustWriteString(query, ")\"")

			// Write the common labels.
			for j := 0; j < len(labels); j++ {
				mustWriteString(query, ",")

				l := labels[j]
				mustWriteString(query, l.name)
				mustWriteString(query, "=\"")
				mustWriteString(query, l.value)
				mustWriteString(query, "\"")
			}

			if q.Aggregation != "" {
				mustWriteString(query, "})")
			}

			q.executeQuery(query, false)
		}()
	}
}

func (q *queryExecutor) executeQuery(query *strings.Builder, retResult bool) []byte {
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
		return nil
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
		return nil
	}

	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode/100 != 2 {
		q.Logger.Warn("response from query non-2XX status code",
			zap.Int("code", resp.StatusCode))
	}

	if q.Debug || retResult {
		reader := io.Reader(resp.Body)
		if q.Debug && q.DebugLength > 0 {
			reader = io.LimitReader(resp.Body, int64(q.DebugLength))
		}

		data, err := ioutil.ReadAll(reader)
		if err != nil {
			q.Logger.Error("failed to read response body",
				zap.Error(err))
		}

		if retResult {
			return data
		}

		q.Logger.Info("response body",
			zap.Int("limit", q.DebugLength),
			zap.ByteString("body", data))
	}

	return nil
}

type PromQueryResult struct {
	Status string        `json:"status"`
	Data   PromQueryData `json:"data"`
}

type PromQueryData struct {
	ResultType promql.ValueType  `json:"resultType"`
	Result     []PromQueryMatrix `json:"result"`
	Stats      *stats.QueryStats `json:"stats,omitempty"`
}

type PromQueryMatrix struct {
	Values []model.SamplePair `json:"values"`
}

func (q *queryExecutor) validateQuery(dps Datapoints, data []byte) bool {
	res := PromQueryResult{}
	err := json.Unmarshal(data, &res)
	if err != nil {
		q.Logger.Error("unable to unmarshal PromQL query result",
			zap.Error(err))
		return false
	}

	matrix := res.Data.Result
	if len(matrix) != 1 {
		q.Logger.Error("expecting one result series, but got "+strconv.Itoa(len(matrix)),
			zap.Any("results", matrix))
		return false
	}

	i, matches := 0, 0

	for _, value := range matrix[0].Values {
		for i < len(dps) {
			if float64(value.Value) == dps[i].Value {
				matches++
				i++
				break
			}

			i++
		}

		if i == len(dps) {
			break
		}
	}

	if matches == 0 {
		q.Logger.Error("no values matched at all")
		return false
	}

	return true
}

func mustWriteString(w *strings.Builder, v string) {
	_, err := w.WriteString(v)
	if err != nil {
		panic(err)
	}
}
