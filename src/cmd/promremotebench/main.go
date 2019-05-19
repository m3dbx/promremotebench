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
	"flag"
	"log"
	"os"
	"time"
	"strconv"

	"promremotebench/pkg/generators"
)

const (
	envTarget = "PROMREMOTEBENCH_TARGET"
	envInterval = "PROMREMOTEBENCH_INTERVAL"
	envNumHosts = "PROMREMOTEBENCH_NUM_HOSTS"
	envRemoteBatchSize = "PROMREMOTEBENCH_BATCH"
)

func main() {
	var (
		targetURL             = flag.String("target", "http://localhost:7201/receive", "Target remote write endpoint")
		scrapeIntervalSeconds = flag.Int("interval", 10, "Prom endpoint scrape interval")
		numHosts              = flag.Int("hosts", 100, "Number of hosts to mimic scrapes from")
		remoteBatchSize       = flag.Int("batch", 128, "Number of metrics per batch send via remote write")
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

	now := time.Now()
	hostGen := generators.NewHostsSimulator(*numHosts, now)
	client, err := NewClient(*targetURL, time.Minute)
	if err != nil {
		log.Fatalf("Error creating remote client: %v", err)
	}

	generateLoop(hostGen, *scrapeIntervalSeconds, client, *remoteBatchSize)
}

func generateLoop(
	generator *generators.HostsSimulator, 
	intervalSeconds int,
	remotePromClient *Client, 
	remotePromBatchSize int,
) {
	period := time.Duration(intervalSeconds) * time.Second

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	// First with zero progression
	remoteWrite(generator.Generate(0), remotePromClient, 
		remotePromBatchSize)	

	for range ticker.C {
		go func() {
			// Progress each time by same period
			remoteWrite(generator.Generate(period), 
				remotePromClient, remotePromBatchSize)
		}()
	}
}
