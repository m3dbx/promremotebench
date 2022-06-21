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
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"time"

	"promremotebench/pkg/generators"

	xconfig "github.com/m3db/m3/src/x/config"
	"github.com/m3db/m3/src/x/instrument"
	xos "github.com/m3db/m3/src/x/os"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	tallyprom "github.com/uber-go/tally/prometheus"
	"go.uber.org/zap"
)

const (
	envWrite                     = "PROMREMOTEBENCH_WRITE"
	envQuery                     = "PROMREMOTEBENCH_QUERY"
	envTarget                    = "PROMREMOTEBENCH_TARGET"
	envTargetHeadersJSON         = "PROMREMOTEBENCH_TARGET_HEADERS_JSON"
	envScrapeServer              = "PROMREMOTEBENCH_SCRAPE_SERVER"
	envQueryTarget               = "PROMREMOTEBENCH_QUERY_TARGET"
	envInterval                  = "PROMREMOTEBENCH_INTERVAL"
	envNumHosts                  = "PROMREMOTEBENCH_NUM_HOSTS"
	envRemoteBatchSize           = "PROMREMOTEBENCH_BATCH"
	envNewSeriesPercent          = "PROMREMOTEBENCH_NEW_SERIES_PERCENTAGE"
	envLabelsJSON                = "PROMREMOTEBENCH_LABELS_JSON"
	envLabelsJSONEnv             = "PROMREMOTEBENCH_LABELS_JSON_ENV"
	envSyntheticLabelCardinality = "PROMREMOTEBENCH_SYNTHETIC_LABEL_CARDINALITY"
	envCheckerTick               = "PROMREMOTEBENCH_CHECKER_TICK"
	envCheckerExpiration         = "PROMREMOTEBENCH_CHECKER_EXPIRATION"
	envCheckerTargetLen          = "PROMREMOTEBENCH_CHECKER_TARGET_LEN"
	envQueryConcurrency          = "PROMREMOTEBENCH_QUERY_CONCURRENCY"
	envQueryNumSeries            = "PROMREMOTEBENCH_QUERY_NUM_SERIES"
	envQueryLoadStep             = "PROMREMOTEBENCH_QUERY_LOAD_STEP"
	envQueryLoadRange            = "PROMREMOTEBENCH_QUERY_LOAD_RANGE"
	envQuerySkipAccuracy         = "PROMREMOTEBENCH_QUERY_SKIP_ACCURACY"
	envQueryAccuracyStep         = "PROMREMOTEBENCH_QUERY_ACCURACY_STEP"
	envQueryAccuracyRange        = "PROMREMOTEBENCH_QUERY_ACCURACY_RANGE"
	envQueryAggregation          = "PROMREMOTEBENCH_QUERY_AGGREGATION"
	envQueryLabelsJSON           = "PROMREMOTEBENCH_QUERY_LABELS_JSON"
	envQueryLabelsJSONEnv        = "PROMREMOTEBENCH_QUERY_LABELS_JSON_ENV"
	envQueryHeaders              = "PROMREMOTEBENCH_QUERY_HEADERS_JSON"
	envQuerySleep                = "PROMREMOTEBENCH_QUERY_SLEEP"
	envQueryDebug                = "PROMREMOTEBENCH_QUERY_DEBUG"
	envQueryDebugLength          = "PROMREMOTEBENCH_QUERY_DEBUG_LENGTH"
	envColdWritesPercent         = "PROMREMOTEBENCH_COLD_WRITES_PERCENTAGE"
	envColdWritesRange           = "PROMREMOTEBENCH_COLD_WRITES_RANGE"

	// maxNumScrapesActive determines how many scrapes
	// at max to allow be active (fall behind by)
	maxNumScrapesActive = 4
)

var (
	defaultMetricsSanitization = instrument.PrometheusMetricSanitization
	defaultMetricsExtended     = instrument.DetailedExtendedMetrics
	defaultConfiguration       = Configuration{
		Metrics: instrument.MetricsConfiguration{
			Sanitization: &defaultMetricsSanitization,
			SamplingRate: 1,
			PrometheusReporter: &tallyprom.Configuration{
				HandlerPath: "/metrics",
			},
			ExtendedMetrics: &defaultMetricsExtended,
		},
	}
)

type targetUrls []string

func (u *targetUrls) String() string {
	return "a slice of target urls"
}

// Set satisfies flag.Value; this will expand a comma separated list of values.
func (u *targetUrls) Set(value string) error {
	*u = strings.Split(value, ",")
	return nil
}

type headers []header

type header struct {
	name  string
	value string
}

func (h *headers) String() string {
	var r [][]string
	for _, v := range []header(*h) {
		r = append(r, []string{v.name, v.value})
	}
	return fmt.Sprintf("%v", r)
}

func (h *headers) Set(value string) error {
	firstSplit := strings.Index(value, ":")
	if firstSplit == -1 {
		return fmt.Errorf("header missing separating colon: '%v'", value)
	}

	*h = append(*h, header{
		name:  strings.TrimSpace(value[:firstSplit]),
		value: strings.TrimSpace(value[firstSplit+1:]),
	})

	return nil
}

func main() {
	var (
		write = flag.Bool("write", true, "enable write load benchmarking")
		query = flag.Bool("query", false, "enable query load benchmarking")

		// config filename
		configFile = flag.String("config", "", "Location of the config file")

		// write options
		scrapeServer          = flag.String("scrape-server", "", "Listen address for scrape HTTP server (instead of remote write)")
		numHosts              = flag.Int("hosts", 100, "Number of hosts to mimic scrapes from")
		labels                = flag.String("labels", "{}", "Labels in JSON format to append to all metrics")
		labelsFromEnv         = flag.String("labels-env", "{}", "Labels in JSON format, with the string values as environment variable names, to append to all metrics")
		newSeriesPercent      = flag.Float64("new", 0.01, "Factor of new series per scrape interval [0.0, 1.0]")
		scrapeIntervalSeconds = flag.Float64("interval", 10.0, "Prom endpoint scrape interval in seconds (for remote write)")
		remoteBatchSize       = flag.Int("batch", 128, "Number of metrics per batch send via remote write (for remote write)")
		scrapeSpreadBy        = flag.Float64("spread", 10.0, "The number of times to spread the scrape interval by when emitting samples (for remote write)")

		syntheticLabelCardinality = flag.Uint64("synthetic-label-cardinality", 0, "Global cardinality of synthetic label (for generating high cardinality `synthetic_label` label if specified)")

		// cold write options
		coldWritesPercent = flag.Float64("cold-percent", 0, "Percentage of cold writes [0.0, 1.0]")
		coldWritesRange   = flag.Duration("cold-range", 5*24*time.Hour, "Range of cold writes (duration)")

		// checker options
		checkerTick       = flag.Duration("checker-tick", time.Minute, "Checker tick for trimming values and series")
		checkerExpiration = flag.Duration("checker-expiration", time.Minute, "Checker threshold to expire stale series")
		checkerTargetLen  = flag.Int("checker-target-len", 10, "Target length of values to be stored by checker")

		// query options
		queryConcurrency   = flag.Int("query-concurrency", 10, "Query concurrency value")
		queryNumSeries     = flag.Int("query-num-series", 500, "Query number of series (will round up to nearest 100), cannot exceed the number of 101*write_num_hosts (since each host sends 101 metrics)")
		queryLoadStep      = flag.Duration("query-load-step", time.Minute, "Query step size")
		queryLoadRange     = flag.Duration("query-load-range", 12*time.Hour, "Query time range size (from now backwards)")
		querySkipAccuracy  = flag.Bool("query-skip-accuracy-checks", false, "Skip accuracy checks in query load")
		queryAccuracyStep  = flag.Duration("query-accuracy-step", 10*time.Second, "Accuracy query step size")
		queryAccuracyRange = flag.Duration("query-accuracy-range", 35*time.Second, "Accuracy query time range size (from now backwards)")
		queryAggregation   = flag.String("query-aggregation", "sum", "Query aggregation")
		queryLabels        = flag.String("query-labels", "{}", "Labels in JSON format to use in all queries")
		queryLabelsFromEnv = flag.String("query-labels-env", "{}", "Labels in JSON format, with the string values as environment variable names, to use in all queries")
		queryHeaders       = flag.String("query-headers", "{}", "Query headers in JSON format to send with each request (can be a JSON array for multiple query target URLs)")
		querySleep         = flag.Duration("query-sleep", time.Second, "Query time to sleep between finishing one query and executing the next")
		queryDebug         = flag.Bool("query-debug", false, "Query debug flag to print out the first few characters of a query response")
		queryDebugLength   = flag.Int("query-debug-length", 128, "Query debug character length to print, use zero to print entire response")
	)

	// Can have multiple write-targets.
	var writeTargetURLs targetUrls
	flag.Var(&writeTargetURLs, "target", "Target remote write endpoint(s) (for remote write)")

	var writeTargetHeaders headers
	flag.Var(&writeTargetHeaders, "target-header", "Header to set with remote write request, can specify many, e.g. 'User-Agent: Foo'")

	// Can have multiple query-targets.
	var queryTargetURLs targetUrls
	flag.Var(&queryTargetURLs, "query-target", "Target query endpoint(s) (for exercising by proxy remote read)")
	flag.Parse()

	var (
		logger = instrument.NewOptions().Logger()
	)

	cfg := defaultConfiguration
	if len(*configFile) != 0 {
		if err := xconfig.LoadFile(&cfg, *configFile, xconfig.Options{}); err != nil {
			logger.Fatal("unable to load config file",
				zap.String("configFile", *configFile),
				zap.Error(err))
		}
	}

	scope, _, err := cfg.Metrics.NewRootScope()
	if err != nil {
		logger.Fatal("unable to create root metrics scope",
			zap.Error(err))
	}

	if len(writeTargetURLs) == 0 {
		writeTargetURLs = targetUrls{"http://localhost:7201/receive"}
	}

	if len(queryTargetURLs) == 0 {
		queryTargetURLs = targetUrls{"http://localhost:7201/api/v1/query_range"}
	}

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
		writeTargetURLs = strings.Split(v, ",")
	}
	if v := os.Getenv(envTargetHeadersJSON); v != "" {
		values := make(map[string]string)
		err := json.Unmarshal([]byte(v), &values)
		if err != nil {
			logger.Fatal("could not parse env var",
				zap.String("var", envTargetHeadersJSON), zap.Error(err))
		}
		for k, v := range values {
			writeTargetHeaders = append(writeTargetHeaders, header{name: k, value: v})
		}
	}
	if v := os.Getenv(envScrapeServer); v != "" {
		*scrapeServer = v
	}
	if v := os.Getenv(envQueryTarget); v != "" {
		queryTargetURLs = strings.Split(v, ",")
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

	if v := os.Getenv(envSyntheticLabelCardinality); v != "" {
		*syntheticLabelCardinality, err = strconv.ParseUint(v, 10, 64)
		if err != nil {
			logger.Fatal("could not parse env var",
				zap.String("var", envSyntheticLabelCardinality), zap.Error(err))
		}
	}
	if *syntheticLabelCardinality < 0 {
		logger.Fatal("synthetic label cardinality must not be negative",
			zap.Uint64("value", *syntheticLabelCardinality))
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
	if v := os.Getenv(envQuerySkipAccuracy); v != "" {
		*querySkipAccuracy, err = strconv.ParseBool(v)
		if err != nil {
			logger.Fatal("could not parse env var",
				zap.String("var", envQuerySkipAccuracy), zap.Error(err))
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

	if v := os.Getenv(envColdWritesPercent); v != "" {
		*coldWritesPercent, err = strconv.ParseFloat(v, 64)
		if err != nil {
			logger.Fatal("could not parse env var",
				zap.String("var", envColdWritesPercent), zap.Error(err))
		}
	}
	if *coldWritesPercent < 0.0 || *coldWritesPercent > 1.0 {
		logger.Fatal("cold writes percentage must be in the range of [0.0, 1.0]",
			zap.Float64("value", *coldWritesPercent))
	}

	if v := os.Getenv(envColdWritesRange); v != "" {
		*coldWritesRange, err = time.ParseDuration(v)
		if err != nil {
			logger.Fatal("could not parse env var",
				zap.String("var", envColdWritesRange), zap.Error(err))
		}
	}

	// Parse opts further.
	writeTargetHeadersMap := make(map[string]string)
	for _, h := range writeTargetHeaders {
		writeTargetHeadersMap[h.name] = h.value
	}

	parsedLabels := parseLabels(*labels, *labelsFromEnv, logger)
	parsedQueryLabels := parseLabels(*queryLabels, *queryLabelsFromEnv, logger)

	if *query && len(parsedQueryLabels) == 0 {
		logger.Warn(`No query labels provided. This will result in more metrics returned than expected
		if there is more than one instance of promremotebench executing queries due to metric name
		duplication across promremotebench instances.`)
	}

	var parsedQueryHeaders map[string]string
	var parsedQueryHeadersPerTarget []map[string]string
	// Try parsing as JSON array...
	err = json.Unmarshal([]byte(*queryHeaders), &parsedQueryHeadersPerTarget)
	if _, ok := err.(*json.UnmarshalTypeError); ok {
		// ... if that failed, try parsing as a single JSON object.
		parsedQueryHeadersPerTarget = nil
		err = json.Unmarshal([]byte(*queryHeaders), &parsedQueryHeaders)
	}
	if err != nil {
		logger.Fatal("could not parse fixed query headers", zap.Error(err))
	}

	queryTargets := make([]queryTarget, len(queryTargetURLs))

	for i, url := range queryTargetURLs {
		headersSource := make(map[string]string)
		if parsedQueryHeadersPerTarget == nil {
			// Use the same headers for each target.
			headersSource = parsedQueryHeaders
		} else if i < len(parsedQueryHeadersPerTarget) {
			// Use the target specific headers from the array.
			headersSource = parsedQueryHeadersPerTarget[i]
		}
		headers := make(map[string]string)
		for k, v := range headersSource {
			headers[k] = v
		}
		queryTargets[i] = queryTarget{
			URL:     url,
			Headers: headers,
		}
	}

	// Create structures.
	var (
		now         = time.Now()
		hostSimOpts = generators.HostsSimulatorOptions{
			Labels:                    parsedLabels,
			SyntheticLabelCardinality: *syntheticLabelCardinality,
			ColdWritesPercent:         *coldWritesPercent,
			ColdWritesRange:           *coldWritesRange,
		}

		hostGen = generators.NewHostsSimulator(*numHosts, now, hostSimOpts)
	)

	if hostSimOpts.ColdWritesPercent > 0 {
		logger.Info("will simulate cold writes",
			zap.Float64("coldWritesPercent", hostSimOpts.ColdWritesPercent),
			zap.Duration("coldWritesRange", hostSimOpts.ColdWritesRange),
		)
	}

	if hostSimOpts.SyntheticLabelCardinality > 0 {
		logger.Info("will emit synthetic label",
			zap.String("label", generators.SyntheticLabelName),
			zap.Uint64("cardinality", hostSimOpts.SyntheticLabelCardinality),
		)
	}

	client, err := NewClient(writeTargetURLs, time.Minute, scope.SubScope("writes"))
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
	}, *numHosts)

	// Start workloads.
	if *write {
		go func() {
			scrapeDuration := *scrapeIntervalSeconds * float64(time.Second)
			if *scrapeServer == "" {
				logger.Info("starting remote write load")
				progressBy := scrapeDuration / *scrapeSpreadBy
				writeLoop(hostGen, time.Duration(scrapeDuration),
					time.Duration(progressBy), *newSeriesPercent,
					client, *remoteBatchSize, writeTargetHeadersMap,
					logger, checker)
			} else {
				logger.Info("starting scrape server", zap.String("address", *scrapeServer))
				gatherer := newGatherer(hostGen, time.Duration(scrapeDuration),
					*newSeriesPercent, logger, checker)
				handler := promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{
					ErrorHandling: promhttp.ContinueOnError,
					ErrorLog:      newPromHTTPErrorLogger(logger),
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
				Targets:       queryTargets,
				Concurrency:   *queryConcurrency,
				NumWriteHosts: *numHosts,
				NumSeries:     *queryNumSeries,
				LoadStep:      *queryLoadStep,
				LoadRange:     *queryLoadRange,
				SkipAccuracy:  *querySkipAccuracy,
				AccuracyStep:  *queryAccuracyStep,
				AccuracyRange: *queryAccuracyRange,
				Aggregation:   *queryAggregation,
				Labels:        parsedQueryLabels,
				Sleep:         *querySleep,
				Debug:         *queryDebug,
				DebugLength:   *queryDebugLength,
				Logger:        logger,
				Scope:         scope.SubScope("queries"),
			})
			q.Run(checker)
		}()
	}
	if cfg.DebugListenAddress != "" {
		go func() {
			err := http.ListenAndServe(cfg.DebugListenAddress, nil)
			if err != nil {
				logger.Fatal("debug profile server error", zap.Error(err))
			}
		}()
	}

	xos.WaitForInterrupt(logger, xos.InterruptOptions{})
}

func newPromHTTPErrorLogger(logger *zap.Logger) promhttp.Logger {
	return promHTTPErrorLogger{logger: logger.Sugar()}
}

type promHTTPErrorLogger struct {
	logger *zap.SugaredLogger
}

func (l promHTTPErrorLogger) Println(v ...interface{}) {
	l.logger.Error(v...)
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
