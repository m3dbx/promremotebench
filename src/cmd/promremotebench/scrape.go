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
	"sync"
	"time"

	"promremotebench/pkg/generators"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/pkg/labels"
	"go.uber.org/zap"
)

func newGatherer(
	generator *generators.HostsSimulator,
	scrapeIntervalExpected time.Duration,
	newSeriesPercent float64,
	logger *zap.Logger,
) prometheus.Gatherer {
	return &gatherer{
		generator:              generator,
		scrapeIntervalExpected: scrapeIntervalExpected,
		newSeriesPercent:       newSeriesPercent,
		logger:                 logger,
	}
}

type gatherer struct {
	sync.Mutex
	generator              *generators.HostsSimulator
	scrapeIntervalExpected time.Duration
	newSeriesPercent       float64
	logger                 *zap.Logger
}

func (g *gatherer) Gather() ([]*dto.MetricFamily, error) {
	g.Lock()
	defer g.Unlock()

	interval := g.scrapeIntervalExpected

	series, err := g.generator.Generate(interval, interval,
		g.newSeriesPercent)
	if err != nil {
		g.logger.Fatal("error generating load", zap.Error(err))
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
			g.logger.Fatal("no metric family found for metric")
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
