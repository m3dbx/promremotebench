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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/util/stats"

	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type queryExecutor struct {
	queryExecutorOptions
	client *http.Client

	metrics map[string]queryMetrics
	// Non-URL specific errors
	invalidResponseError tally.Counter
	fanoutFailedError    tally.Counter
}

type queryMetrics struct {
	success tally.Counter

	validationFailedError   tally.Counter
	mismatchedResponseError tally.Counter
}

type queryTarget struct {
	URL     string
	Headers map[string]string
}

type queryExecutorOptions struct {
	Targets       []queryTarget
	Concurrency   int
	NumWriteHosts int
	NumSeries     int
	LoadStep      time.Duration
	LoadRange     time.Duration
	SkipAccuracy  bool
	AccuracyStep  time.Duration
	AccuracyRange time.Duration
	Aggregation   string
	Labels        map[string]string
	Sleep         time.Duration
	Debug         bool
	DebugLength   int
	Logger        *zap.Logger
	Scope         tally.Scope
}

func newQueryExecutor(opts queryExecutorOptions) *queryExecutor {
	scope := opts.Scope
	metrics := make(map[string]queryMetrics)
	errorScope := scope.SubScope("error")
	for _, target := range opts.Targets {
		url := target.URL
		metrics[url] = queryMetrics{
			success: scope.Tagged(map[string]string{
				"url": url,
			}).Counter("success"),
			validationFailedError: errorScope.Tagged(map[string]string{
				"url": url,
			}).Counter("validation-failed"),
			mismatchedResponseError: errorScope.Tagged(map[string]string{
				"url": url,
			}).Counter("mismatched-response"),
		}
	}

	return &queryExecutor{
		queryExecutorOptions: opts,
		client:               http.DefaultClient,
		metrics:              metrics,
		invalidResponseError: errorScope.Counter("invalid-response"),
		fanoutFailedError:    errorScope.Counter("fanout-failed"),
	}
}

func (q *queryExecutor) Run(checker Checker) {
	q.Logger.Info("query load configured",
		zap.Int("concurrency", q.Concurrency))
	for i := 0; i < q.Concurrency; i++ {
		go q.alertLoad(checker)
	}

	if q.SkipAccuracy {
		q.Logger.Info("skipping query accuracy checks")
	} else {
		go q.accuracyCheck(checker)
	}
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
				if i > 0 {
					q.Logger.Error("no hosts returned in the checker, skipping accuracy check.")
				}
				return
			}

			var (
				dps          []Datapoint
				selectedHost string
			)

			for j := 0; j < 5; j++ {
				selectedHost = curHostnames[rand.Intn(len(curHostnames))]
				dps = checker.GetDatapoints(selectedHost)
				if len(dps) > 1 {
					break
				}
			}

			if len(dps) <= 1 && i > 1 {
				q.Logger.Error("couldn't find a host with more than 1 datapoint. Skipping accuracy check")
			}

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

			res, err := q.fanoutQuery(query, true, q.AccuracyRange, q.AccuracyStep)
			if len(res) == 0 {
				q.Logger.Error("invalid response for accuracy query")
				q.invalidResponseError.Inc(1)
			} else if err != nil {
				q.Logger.Error("fanout execution failed", zap.Error(err))
				q.fanoutFailedError.Inc(1)
			} else {
				for url, result := range res {
					if q.validateQuery(dps, result, selectedHost) {
						q.metrics[url].success.Inc(1)
					} else {
						q.metrics[url].validationFailedError.Inc(1)
					}
				}
			}
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

			q.fanoutQuery(query, false, q.LoadRange, q.LoadStep)
		}()
	}
}

func (q *queryExecutor) fanoutQuery(
	query *strings.Builder,
	retResult bool,
	queryRange time.Duration,
	queryStep time.Duration,
) (map[string][]byte, error) {
	now := time.Now()
	values := make(url.Values)
	values.Set("query", query.String())
	values.Set("start", strconv.Itoa(int(now.Add(-1*queryRange).Unix())))
	values.Set("end", strconv.Itoa(int(now.Unix())))
	values.Set("step", strconv.FormatFloat(queryStep.Seconds(), 'f', -1, 64))

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		multiErr xerrors.MultiError
		qs       = values.Encode()
	)

	results := make(map[string][]byte, len(q.Targets))
	for _, target := range q.Targets {
		url := target.URL
		headers := target.Headers
		wg.Add(1)
		reqURL := fmt.Sprintf("%s?%s", url, qs)

		if q.Debug {
			q.Logger.Info("fanout query",
				zap.String("url", reqURL),
				zap.Any("values", values))
		}

		go func() {
			res, err := q.executeQuery(reqURL, headers, retResult)
			mu.Lock()
			multiErr = multiErr.Add(err)
			results[url] = res
			mu.Unlock()
			wg.Done()
		}()
	}

	wg.Wait()

	if err := multiErr.FinalError(); err != nil {
		q.Logger.Error("fanout error", zap.Error(err))
		return results, err
	}

	// NB: If less than 2 results returned, no need to compare for equality.
	if !retResult || len(results) < 2 {
		return results, nil
	}

	var (
		firstResult []byte
	)
	for url, res := range results {
		if firstResult == nil {
			firstResult = res
			continue
		}

		if bytes.Equal(res, firstResult) {
			continue
		}

		q.metrics[url].mismatchedResponseError.Inc(1)
		q.Logger.Error("mismatch in returned data", zap.String("url", url))
		return nil, errors.New("mismatch in returned data")
	}

	return results, nil
}

func (q *queryExecutor) executeQuery(
	reqURL string,
	headers map[string]string,
	retResult bool,
) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request error: %v", err)
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := q.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}

	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode/100 != 2 {
		q.Logger.Warn("response from query non-2XX status code",
			zap.String("url", reqURL),
			zap.Int("code", resp.StatusCode),
		)
	}

	if q.Debug || retResult {
		reader := io.Reader(resp.Body)
		if q.Debug && q.DebugLength > 0 {
			reader = io.LimitReader(resp.Body, int64(q.DebugLength))
		}

		data, err := ioutil.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read response body: %v", err)
		}

		q.Logger.Info("response body",
			zap.Int("limit", q.DebugLength),
			zap.ByteString("body", data))

		if retResult {
			return data, nil
		}
	}

	return nil, nil
}

// PromQueryResult is a prom query result.
type PromQueryResult struct {
	Status string        `json:"status"`
	Data   PromQueryData `json:"data"`
}

// PromQueryData is a prom query data.
type PromQueryData struct {
	ResultType parser.ValueType  `json:"resultType"`
	Result     []PromQueryMatrix `json:"result"`
	Stats      *stats.QueryStats `json:"stats,omitempty"`
}

// PromQueryMatrix is a prom query matrix.
type PromQueryMatrix struct {
	Values []model.SamplePair `json:"values"`
}

func (q *queryExecutor) validateQuery(dps Datapoints, data []byte, hostname string) bool {
	logger := q.Logger.With(zap.String("hostname", hostname))
	res := PromQueryResult{}
	err := json.Unmarshal(data, &res)
	if err != nil {
		logger.Error("unable to unmarshal PromQL query result",
			zap.Error(err))
		return false
	}

	matrix := res.Data.Result
	if len(matrix) != 1 {
		logger.Error("expecting one result series, but got "+strconv.Itoa(len(matrix)),
			zap.Any("results", matrix))
		return false
	}

	i, matches := 0, 0

	if len(matrix[0].Values) == 0 {
		logger.Warn("No results returned from query. There may be a slight delay in ingestion")
		return false
	}

	for _, value := range matrix[0].Values {
		for i < len(dps) {
			if float64(value.Value) == dps[i].Value {
				matches++
				break
			}

			i++
		}

		i = 0
	}

	if matches == 0 {
		logger.Error("no values matched at all.",
			zap.Int("num_written_dps", len(dps)),
			zap.Int("num_query_dps", len(matrix[0].Values)))

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
