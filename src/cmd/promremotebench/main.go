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
	"os"
	"strconv"
	"time"

	"promremotebench/pkg/generators"
)

const (
	envTarget           = "PROMREMOTEBENCH_TARGET"
	envInterval         = "PROMREMOTEBENCH_INTERVAL"
	envNumHosts         = "PROMREMOTEBENCH_NUM_HOSTS"
	envRemoteBatchSize  = "PROMREMOTEBENCH_BATCH"
	envNewSeriesPercent = "PROMREMOTEBENCH_NEW_SERIES_PERCENTAGE"
	envLabelsJSON       = "PROMREMOTEBENCH_LABELS_JSON"
)

func main() {
	var (
		targetURL             = flag.String("target", "http://localhost:7201/receive", "Target remote write endpoint")
		scrapeIntervalSeconds = flag.Int("interval", 10, "Prom endpoint scrape interval")
		numHosts              = flag.Int("hosts", 100, "Number of hosts to mimic scrapes from")
		newSeriesPercent      = flag.Float64("new", 0.01, "Percentage of new series per scrape interval [0, 100]")
		remoteBatchSize       = flag.Int("batch", 128, "Number of metrics per batch send via remote write")
		labels                = flag.String("labels", "{}", "Labels in JSON format to append to all metrics")
	)

	flag.Parse()
	if len(*targetURL) == 0 {
		flag.Usage()
		os.Exit(-1)
	}

	var err error
	if v := os.Getenv(envTarget); v != "" {
		*targetURL = v
	}
	if v := os.Getenv(envInterval); v != "" {
		*scrapeIntervalSeconds, err = strconv.Atoi(v)
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
	if *newSeriesPercent < 0 || *newSeriesPercent > 100 {
		log.Fatalf("new series percentage must be in the range of [0, 100]")
	}
	if v := os.Getenv(envLabelsJSON); v != "" {
		*labels = v
	}

	var parsedLabels map[string]string
	if err := json.Unmarshal([]byte(*labels), &parsedLabels); err != nil {
		log.Fatalf("could not parse fixed added labels: %v", err)
	}

	now := time.Now()
	hostGen := generators.NewHostsSimulator(*numHosts, *scrapeIntervalSeconds, now,
		generators.HostsSimulatorOptions{Labels: parsedLabels})
	client, err := NewClient(*targetURL, time.Minute)
	if err != nil {
		log.Fatalf("error creating remote client: %v", err)
	}

	for _, host := range hostGen.Hosts() {
		log.Println("simulating host", host.Name)
	}

	generateLoop(hostGen, *scrapeIntervalSeconds, *newSeriesPercent,
		client, *remoteBatchSize)
}

func generateLoop(
	generator *generators.HostsSimulator,
	intervalSeconds int,
	newSeriesPercent float64,
	remotePromClient *Client,
	remotePromBatchSize int,
) {
	series := generator.Generate(0, newSeriesPercent)
	remoteWrite(series, remotePromClient, remotePromBatchSize)

	secTick := 1
	for _ = range time.Tick(time.Second) {
		go func(tick int) {
			curTick := tick % intervalSeconds
			series := generator.Generate(curTick, newSeriesPercent)
			remoteWrite(series, remotePromClient, remotePromBatchSize)
		}(secTick)

		secTick++
	}
}
