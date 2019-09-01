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
	"flag"
	"net/http"
	"os"
	"strconv"
	"time"

	"promremotebench/pkg/generators"

	"github.com/m3db/m3/src/x/instrument"
	xos "github.com/m3db/m3/src/x/os"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

const (
	envWrite              = "PROMREMOTEBENCH_WRITE"
	envQuery              = "PROMREMOTEBENCH_QUERY"
	envTarget             = "PROMREMOTEBENCH_TARGET"
	envScrapeServer       = "PROMREMOTEBENCH_SCRAPE_SERVER"
	envQueryTarget        = "PROMREMOTEBENCH_QUERY_TARGET"
	envInterval           = "PROMREMOTEBENCH_INTERVAL"
	envNumHosts           = "PROMREMOTEBENCH_NUM_HOSTS"
	envRemoteBatchSize    = "PROMREMOTEBENCH_BATCH"
	envNewSeriesPercent   = "PROMREMOTEBENCH_NEW_SERIES_PERCENTAGE"
	envLabelsJSON         = "PROMREMOTEBENCH_LABELS_JSON"
	envLabelsJSONEnv      = "PROMREMOTEBENCH_LABELS_JSON_ENV"
	envCheckerTick        = "PROMREMOTEBENCH_CHECKER_TICK"
	envCheckerExpiration  = "PROMREMOTEBENCH_CHECKER_EXPIRATION"
	envCheckerTargetLen   = "PROMREMOTEBENCH_CHECKER_TARGET_LEN"
	envQueryConcurrency   = "PROMREMOTEBENCH_QUERY_CONCURRENCY"
	envQueryNumSeries     = "PROMREMOTEBENCH_QUERY_NUM_SERIES"
	envQueryLoadStep      = "PROMREMOTEBENCH_QUERY_LOAD_STEP"
	envQueryLoadRange     = "PROMREMOTEBENCH_QUERY_LOAD_RANGE"
	envQueryAccuracyStep  = "PROMREMOTEBENCH_QUERY_ACCURACY_STEP"
	envQueryAccuracyRange = "PROMREMOTEBENCH_QUERY_ACCURACY_RANGE"
	envQueryAggregation   = "PROMREMOTEBENCH_QUERY_AGGREGATION"
	envQueryLabelsJSON    = "PROMREMOTEBENCH_QUERY_LABELS_JSON"
	envQueryLabelsJSONEnv = "PROMREMOTEBENCH_QUERY_LABELS_JSON_ENV"
	envQueryHeaders       = "PROMREMOTEBENCH_QUERY_HEADERS_JSON"
	envQuerySleep         = "PROMREMOTEBENCH_QUERY_SLEEP"
	envQueryDebug         = "PROMREMOTEBENCH_QUERY_DEBUG"
	envQueryDebugLength   = "PROMREMOTEBENCH_QUERY_DEBUG_LENGTH"

	// maxNumScrapesActive determines how many scrapes
	// at max to allow be active (fall behind by)
	maxNumScrapesActive = 4
)

func main() {
	var (
		write = flag.Bool("write", true, "enable write load benchmarking")
		query = flag.Bool("query", false, "enable query load benchmarking")

		// write options
		targetURL             = flag.String("target", "http://localhost:7201/receive", "Target remote write endpoint (for remote write)")
		scrapeServer          = flag.String("scrape-server", "", "Listen address for scrape HTTP server (instead of remote write)")
		numHosts              = flag.Int("hosts", 100, "Number of hosts to mimic scrapes from")
		labels                = flag.String("labels", "{}", "Labels in JSON format to append to all metrics")
		labelsFromEnv         = flag.String("labels-env", "{}", "Labels in JSON format, with the string values as environment variable names, to append to all metrics")
		newSeriesPercent      = flag.Float64("new", 0.01, "Factor of new series per scrape interval [0.0, 1.0]")
		scrapeIntervalSeconds = flag.Float64("interval", 10.0, "Prom endpoint scrape interval in seconds (for remote write)")
		remoteBatchSize       = flag.Int("batch", 128, "Number of metrics per batch send via remote write (for remote write)")
		scrapeSpreadBy        = flag.Float64("spread", 10.0, "The number of times to spread the scrape interval by when emitting samples (for remote write)")

		// checker options
		checkerTick       = flag.Duration("checker-tick", time.Minute, "Checker tick for trimming values and series")
		checkerExpiration = flag.Duration("checker-expiration", time.Minute, "Checker threshold to expire stale series")
		checkerTargetLen  = flag.Int("checker-target-len", 10, "Target length of values to be stored by checker")

		// query options
		queryTargetURL     = flag.String("query-target", "http://localhost:7201/api/v1/query_range", "Target query endpoint (for exercising by proxy remote read)")
		queryConcurrency   = flag.Int("query-concurrency", 10, "Query concurrency value")
		queryNumSeries     = flag.Int("query-num-series", 500, "Query number of series (will round up to nearest 100), cannot exceed the number of 101*write_num_hosts (since each host sends 101 metrics)")
		queryLoadStep      = flag.Duration("query-load-step", time.Minute, "Query step size")
		queryLoadRange     = flag.Duration("query-load-range", 12*time.Hour, "Query time range size (from now backwards)")
		queryAccuracyStep  = flag.Duration("query-accuracy-step", 10*time.Second, "Query step size")
		queryAccuracyRange = flag.Duration("query-accuracy-range", 35*time.Second, "Query time range size (from now backwards)")
		queryAggregation   = flag.String("query-aggregation", "sum", "Query aggregation")
		queryLabels        = flag.String("query-labels", "{}", "Labels in JSON format to use in all queries")
		queryLabelsFromEnv = flag.String("query-labels-env", "{}", "Labels in JSON format, with the string values as environment variable names, to use in all queries")
		queryHeaders       = flag.String("query-headers", "{}", "Query headers in JSON format to send with each request")
		querySleep         = flag.Duration("query-sleep", time.Second, "Query time to sleep between finishing one query and executing the next")
		queryDebug         = flag.Bool("query-debug", false, "Query debug flag to print out the first few characters of a query response")
		queryDebugLength   = flag.Int("query-debug-length", 128, "Query debug character length to print, use zero to print entire response")
	)

	flag.Parse()
	if len(*targetURL) == 0 {
		flag.Usage()
		os.Exit(-1)
	}

	var (
		logger = instrument.NewOptions().Logger()
		err    error
	)

	// Parse env var overrides.
	if v := os.Getenv(envWrite); v != "" {
		*write, err = strconv.ParseBool(v)
		if err != nil {
			logger.Fatal("could not parse env var",
				zap.String("var", envWrite), zap.Error(err))
		}
	}
	if v := os.Getenv(envQuery); v != "" {
		*query, err = strconv.ParseBool(v)
		if err != nil {
			logger.Fatal("could not parse env var",
				zap.String("var", envQuery), zap.Error(err))
		}
	}
	if v := os.Getenv(envTarget); v != "" {
		*targetURL = v
	}
	if v := os.Getenv(envScrapeServer); v != "" {
		*scrapeServer = v
	}
	if v := os.Getenv(envQueryTarget); v != "" {
		*queryTargetURL = v
	}
	if v := os.Getenv(envInterval); v != "" {
		*scrapeIntervalSeconds, err = strconv.ParseFloat(v, 64)
		if err != nil {
			logger.Fatal("could not parse env var",
				zap.String("var", envInterval), zap.Error(err))
		}
	}
	if v := os.Getenv(envNumHosts); v != "" {
		*numHosts, err = strconv.Atoi(v)
		if err != nil {
			logger.Fatal("could not parse env var",
				zap.String("var", envNumHosts), zap.Error(err))
		}
	}
	if v := os.Getenv(envRemoteBatchSize); v != "" {
		*remoteBatchSize, err = strconv.Atoi(v)
		if err != nil {
			logger.Fatal("could not parse env var",
				zap.String("var", envRemoteBatchSize), zap.Error(err))
		}
	}
	if v := os.Getenv(envNewSeriesPercent); v != "" {
		*newSeriesPercent, err = strconv.ParseFloat(v, 64)
		if err != nil {
			logger.Fatal("could not parse env var",
				zap.String("var", envNewSeriesPercent), zap.Error(err))
		}
	}
	if *newSeriesPercent < 0.0 || *newSeriesPercent > 1.0 {
		logger.Fatal("new series percentage must be in the range of [0.0, 1.0]",
			zap.Float64("value", *newSeriesPercent))
	}
	if v := os.Getenv(envLabelsJSON); v != "" {
		*labels = v
	}
	if v := os.Getenv(envLabelsJSONEnv); v != "" {
		*labelsFromEnv = v
	}
	if v := os.Getenv(envQueryConcurrency); v != "" {
		*queryConcurrency, err = strconv.Atoi(v)
		if err != nil {
			logger.Fatal("could not parse env var",
				zap.String("var", envQueryConcurrency), zap.Error(err))
		}
	}
	if v := os.Getenv(envQueryNumSeries); v != "" {
		*queryNumSeries, err = strconv.Atoi(v)
		if err != nil {
			logger.Fatal("could not parse env var",
				zap.String("var", envQueryNumSeries), zap.Error(err))
		}
	}
	if v := os.Getenv(envQueryLoadStep); v != "" {
		*queryLoadStep, err = time.ParseDuration(v)
		if err != nil {
			logger.Fatal("could not parse env var",
				zap.String("var", envQueryLoadStep), zap.Error(err))
		}
	}
	if v := os.Getenv(envQueryLoadRange); v != "" {
		*queryLoadRange, err = time.ParseDuration(v)
		if err != nil {
			logger.Fatal("could not parse env var",
				zap.String("var", envQueryLoadRange), zap.Error(err))
		}
	}
	if v := os.Getenv(envQueryAccuracyStep); v != "" {
		*queryAccuracyStep, err = time.ParseDuration(v)
		if err != nil {
			logger.Fatal("could not parse env var",
				zap.String("var", envQueryAccuracyStep), zap.Error(err))
		}
	}
	if v := os.Getenv(envQueryAccuracyRange); v != "" {
		*queryAccuracyRange, err = time.ParseDuration(v)
		if err != nil {
			logger.Fatal("could not parse env var",
				zap.String("var", envQueryAccuracyRange), zap.Error(err))
		}
	}
	if v := os.Getenv(envQueryAggregation); v != "" {
		*queryAggregation = v
	}
	if v := os.Getenv(envQueryLabelsJSON); v != "" {
		*queryLabels = v
	}
	if v := os.Getenv(envQueryLabelsJSONEnv); v != "" {
		*queryLabelsFromEnv = v
	}
	if v := os.Getenv(envQueryHeaders); v != "" {
		*queryHeaders = v
	}
	if v := os.Getenv(envQuerySleep); v != "" {
		*querySleep, err = time.ParseDuration(v)
		if err != nil {
			logger.Fatal("could not parse env var",
				zap.String("var", envQuerySleep), zap.Error(err))
		}
	}
	if v := os.Getenv(envQueryDebug); v != "" {
		*queryDebug, err = strconv.ParseBool(v)
		if err != nil {
			logger.Fatal("could not parse env var",
				zap.String("var", envQueryDebug), zap.Error(err))
		}
	}
	if v := os.Getenv(envQueryDebugLength); v != "" {
		*queryDebugLength, err = strconv.Atoi(v)
		if err != nil {
			logger.Fatal("could not parse env var",
				zap.String("var", envQueryDebugLength), zap.Error(err))
		}
	}
	if v := os.Getenv(envCheckerTick); v != "" {
		*checkerTick, err = time.ParseDuration(v)
		if err != nil {
			logger.Fatal("could not parse env var",
				zap.String("var", envCheckerTick), zap.Error(err))
		}
	}
	if v := os.Getenv(envCheckerExpiration); v != "" {
		*checkerExpiration, err = time.ParseDuration(v)
		if err != nil {
			logger.Fatal("could not parse env var",
				zap.String("var", envCheckerExpiration), zap.Error(err))
		}
	}
	if v := os.Getenv(envCheckerTargetLen); v != "" {
		*checkerTargetLen, err = strconv.Atoi(v)
		if err != nil {
			logger.Fatal("could not parse env var",
				zap.String("var", envCheckerTargetLen), zap.Error(err))
		}
	}

	// Parse opts further.
	parsedLabels := parseLabels(*labels, *labelsFromEnv, logger)
	parsedQueryLabels := parseLabels(*queryLabels, *queryLabelsFromEnv, logger)

	if *query && len(parsedQueryLabels) == 0 {
		logger.Warn(`No query labels provided. This will result in more metrics returned than expected
		if there is more than one instance of promremotebench executing queries due to metric name
		duplication across promremotebench instances.`)
	}

	var parsedQueryHeaders map[string]string
	if err := json.Unmarshal([]byte(*queryHeaders), &parsedQueryHeaders); err != nil {
		logger.Fatal("could not parse fixed query headers", zap.Error(err))
	}

	// Create structures.
	now := time.Now()
	hostGen := generators.NewHostsSimulator(*numHosts, now,
		generators.HostsSimulatorOptions{Labels: parsedLabels})
	client, err := NewClient(*targetURL, time.Minute)
	if err != nil {
		logger.Fatal("error creating remote client",
			zap.Error(err))
	}

	var hosts []string
	for _, host := range hostGen.Hosts() {
		hosts = append(hosts, string(host.Name))
	}
	logger.Info("simulating hosts",
		zap.Strings("hosts", hosts))

	checker := newChecker(checkerOptions{
		aggFunc:               sumFunc,
		cleanTickDuration:     *checkerTick,
		expiredSeriesDuration: *checkerExpiration,
		targetLen:             *checkerTargetLen,
	})

	// Start workloads.
	if *write {
		go func() {
			scrapeDuration := *scrapeIntervalSeconds * float64(time.Second)
			if *scrapeServer == "" {
				logger.Info("starting remote write load")
				progressBy := scrapeDuration / *scrapeSpreadBy
				writeLoop(hostGen, time.Duration(scrapeDuration),
					time.Duration(progressBy), *newSeriesPercent,
					client, *remoteBatchSize, logger, checker)
			} else {
				logger.Info("starting scrape server", zap.String("address", *scrapeServer))
				gatherer := newGatherer(hostGen, time.Duration(scrapeDuration),
					*newSeriesPercent, logger, checker)
				handler := promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{
					ErrorHandling: promhttp.PanicOnError,
				})
				err := http.ListenAndServe(*scrapeServer, handler)
				if err != nil {
					logger.Fatal("scrape server error", zap.Error(err))
				}
			}
		}()
	}

	if *query {
		go func() {
			logger.Info("starting query load")
			q := newQueryExecutor(queryExecutorOptions{
				URL:           *queryTargetURL,
				Concurrency:   *queryConcurrency,
				NumWriteHosts: *numHosts,
				NumSeries:     *queryNumSeries,
				LoadStep:      *queryLoadStep,
				LoadRange:     *queryLoadRange,
				AccuracyStep:  *queryAccuracyStep,
				AccuracyRange: *queryAccuracyRange,
				Aggregation:   *queryAggregation,
				Labels:        parsedQueryLabels,
				Headers:       parsedQueryHeaders,
				Sleep:         *querySleep,
				Debug:         *queryDebug,
				DebugLength:   *queryDebugLength,
				Logger:        logger,
			})
			q.Run(checker)
		}()
	}

	xos.WaitForInterrupt(logger, xos.InterruptOptions{})
}

func parseLabels(
	labelsJSON string,
	lablesFromEnvJSON string,
	logger *zap.Logger,
) map[string]string {
	var parsedLabels map[string]string
	if err := json.Unmarshal([]byte(labelsJSON), &parsedLabels); err != nil {
		logger.Fatal("could not parse fixed set labels", zap.Error(err))
	}

	var parsedLabelsFromEnv map[string]string
	if err := json.Unmarshal([]byte(lablesFromEnvJSON), &parsedLabelsFromEnv); err != nil {
		logger.Fatal("could not parse fixed env labels", zap.Error(err))
	}
	for k, v := range parsedLabelsFromEnv {
		envValue := os.Getenv(v)
		if envValue == "" {
			logger.Fatal("label value for env label not set",
				zap.String("label", k),
				zap.String("var", v))
		}
		parsedLabels[k] = envValue
	}

	return parsedLabels
}
