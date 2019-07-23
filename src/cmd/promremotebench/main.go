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
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"promremotebench/pkg/generators"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/pkg/labels"
)

const (
	envScrapeServer     = "PROMREMOTEBENCH_SCRAPE_SERVER"
	envTarget           = "PROMREMOTEBENCH_TARGET"
	envInterval         = "PROMREMOTEBENCH_INTERVAL"
	envNumHosts         = "PROMREMOTEBENCH_NUM_HOSTS"
	envRemoteBatchSize  = "PROMREMOTEBENCH_BATCH"
	envNewSeriesPercent = "PROMREMOTEBENCH_NEW_SERIES_PERCENTAGE"
	envLabelsJSON       = "PROMREMOTEBENCH_LABELS_JSON"

	// maxNumScrapesActive determines how many scrapes
	// at max to allow be active (fall behind by)
	maxNumScrapesActive = 4
)

func main() {
	var (
		scrapeServer          = flag.String("scrape-server", "", "Listen address for scrape HTTP server (instead of remote write)")
		targetURL             = flag.String("target", "http://localhost:7201/receive", "Target remote write endpoint (for remote write)")
		numHosts              = flag.Int("hosts", 100, "Number of hosts to mimic scrapes from")
		labels                = flag.String("labels", "{}", "Labels in JSON format to append to all metrics")
		newSeriesPercent      = flag.Float64("new", 0.01, "Factor of new series per scrape interval [0.0, 1.0]")
		scrapeIntervalSeconds = flag.Float64("interval", 10.0, "Prom endpoint scrape interval in seconds (for remote write)")
		remoteBatchSize       = flag.Int("batch", 128, "Number of metrics per batch send via remote write (for remote write)")
		scrapeSpreadBy        = flag.Float64("spread", 10.0, "The number of times to spread the scrape interval by when emitting samples (for remote write)")
	)

	flag.Parse()
	if len(*targetURL) == 0 {
		flag.Usage()
		os.Exit(-1)
	}

	var err error
	if v := os.Getenv(envScrapeServer); v != "" {
		*scrapeServer = v
	}
	if v := os.Getenv(envTarget); v != "" {
		*targetURL = v
	}
	if v := os.Getenv(envInterval); v != "" {
		*scrapeIntervalSeconds, err = strconv.ParseFloat(v, 64)
		if err != nil {
			log.Fatalf("could not parse env var: var=%s, err=%s", envInterval, err)
		}
	}
	if v := os.Getenv(envNumHosts); v != "" {
		*numHosts, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("could not parse env var: var=%s, err=%s", envNumHosts, err)
		}
	}
	if v := os.Getenv(envRemoteBatchSize); v != "" {
		*remoteBatchSize, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("could not parse env var: var=%s, err=%s", envRemoteBatchSize, err)
		}
	}
	if v := os.Getenv(envNewSeriesPercent); v != "" {
		*newSeriesPercent, err = strconv.ParseFloat(v, 64)
		if err != nil {
			log.Fatalf("could not parse env var: var=%s, err=%s", envNewSeriesPercent, err)
		}
	}
	if *newSeriesPercent < 0.0 || *newSeriesPercent > 1.0 {
		log.Fatalf("new series percentage must be in the range of [0.0, 1.0]")
	}
	if v := os.Getenv(envLabelsJSON); v != "" {
		*labels = v
	}

	var parsedLabels map[string]string
	if err := json.Unmarshal([]byte(*labels), &parsedLabels); err != nil {
		log.Fatalf("could not parse fixed added labels: %v", err)
	}

	now := time.Now()
	hostGen := generators.NewHostsSimulator(*numHosts, now,
		generators.HostsSimulatorOptions{Labels: parsedLabels})
	client, err := NewClient(*targetURL, time.Minute)
	if err != nil {
		log.Fatalf("error creating remote client: %v", err)
	}

	for _, host := range hostGen.Hosts() {
		log.Println("simulating host", host.Name)
	}

	scrapeDuration := *scrapeIntervalSeconds * float64(time.Second)
	if *scrapeServer == "" {
		log.Println("starting remote write load")
		progressBy := scrapeDuration / *scrapeSpreadBy
		generateLoop(hostGen, time.Duration(scrapeDuration),
			time.Duration(progressBy), *newSeriesPercent, client, *remoteBatchSize)
	} else {
		log.Println("starting scrape server", *scrapeServer)
		gatherer := newGatherer(hostGen,
			time.Duration(scrapeDuration), *newSeriesPercent)
		handler := promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{
			ErrorHandling: promhttp.PanicOnError,
		})
		err := http.ListenAndServe(*scrapeServer, handler)
		if err != nil {
			log.Fatalf("scrape server error: %v", err)
		}
	}
}

func generateLoop(
	generator *generators.HostsSimulator,
	scrapeDuration time.Duration,
	progressBy time.Duration,
	newSeriesPercent float64,
	remotePromClient *Client,
	remotePromBatchSize int,
) {
	numWorkers := maxNumScrapesActive *
		int(math.Ceil(float64(scrapeDuration)/float64(progressBy)))
	workers := make(chan struct{}, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workers <- struct{}{}
	}

	ticker := time.NewTicker(progressBy)
	defer ticker.Stop()

	for range ticker.C {
		series, err := generator.Generate(progressBy, scrapeDuration, newSeriesPercent)
		if err != nil {
			log.Fatalf("error generating load: %v", err)
		}

		select {
		case token := <-workers:
			go func() {
				remoteWrite(series, remotePromClient, remotePromBatchSize)
				workers <- token
			}()
		default:
			// Too many active workers
		}
	}
}

func newGatherer(
	generator *generators.HostsSimulator,
	scrapeIntervalExpected time.Duration,
	newSeriesPercent float64,
) prometheus.Gatherer {
	return &gatherer{
		generator:              generator,
		scrapeIntervalExpected: scrapeIntervalExpected,
		newSeriesPercent:       newSeriesPercent,
	}
}

type gatherer struct {
	sync.Mutex
	generator              *generators.HostsSimulator
	scrapeIntervalExpected time.Duration
	newSeriesPercent       float64
}

func (g *gatherer) Gather() ([]*dto.MetricFamily, error) {
	g.Lock()
	defer g.Unlock()

	interval := g.scrapeIntervalExpected

	series, err := g.generator.Generate(interval, interval,
		g.newSeriesPercent)
	if err != nil {
		log.Fatalf("error generating load: %v", err)
	}

	families := make(map[string]*dto.MetricFamily)
	gauge := dto.MetricType_GAUGE
	for i := range series {
		var family *dto.MetricFamily

		for j := range series[i].Labels {
			name := series[i].Labels[j].Name
			if name == labels.MetricName {
				var ok bool
				family, ok = families[name]
				if !ok {
					family = &dto.MetricFamily{
						Name: &name,
						Type: &gauge,
					}
					families[name] = family
				}
				break
			}
		}

		if family == nil {
			log.Fatal("no metric family found for metric")
		}

		labels := make([]*dto.LabelPair, 0, len(series[i].Labels))
		for j := range series[i].Labels {
			label := &dto.LabelPair{
				Name:  &series[i].Labels[j].Name,
				Value: &series[i].Labels[j].Value,
			}
			labels = append(labels, label)
		}

		for _, sample := range series[i].Samples {
			metric := &dto.Metric{
				Label: labels,
				Gauge: &dto.Gauge{
					Value: &sample.Value,
				},
			}
			family.Metric = append(family.Metric, metric)
		}
	}

	results := make([]*dto.MetricFamily, 0, len(families))
	for _, family := range families {
		results = append(results, family)
	}
	return results, nil
}
