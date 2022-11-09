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
	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/zap"
)

func newGatherer(
	generator generators.HostsSimulator,
	scrapeIntervalExpected time.Duration,
	newSeriesPercent float64,
	logger *zap.Logger,
	checker Checker,
) prometheus.Gatherer {
	return &gatherer{
		generator:              generator,
		scrapeIntervalExpected: scrapeIntervalExpected,
		newSeriesPercent:       newSeriesPercent,
		logger:                 logger,
		checker:                checker,
	}
}

type gatherer struct {
	sync.Mutex
	generator              generators.HostsSimulator
	scrapeIntervalExpected time.Duration
	newSeriesPercent       float64
	logger                 *zap.Logger
	checker                Checker
}

func (g *gatherer) Gather() ([]*dto.MetricFamily, error) {
	g.Lock()
	defer g.Unlock()

	interval := g.scrapeIntervalExpected

	hostSeries, err := g.generator.Generate(interval, interval, g.newSeriesPercent)
	if err != nil {
		g.logger.Fatal("error generating load", zap.Error(err))
	}

	g.checker.Store(hostSeries)

	families := make(map[string]*dto.MetricFamily)
	gauge := dto.MetricType_GAUGE
	// @martinm - better to concatenate the results of series or just loop through the keys?
	for _, series := range hostSeries {
		for i := range series {
			var family *dto.MetricFamily

			for j := range series[i].Labels {
				labelName := series[i].Labels[j].Name
				if labelName == labels.MetricName {
					metricName := series[i].Labels[j].Value
					var ok bool
					family, ok = families[metricName]
					if !ok {
						family = &dto.MetricFamily{
							Name: &metricName,
							Type: &gauge,
						}
						families[metricName] = family
					}
					break
				}
			}

			if family == nil {
				g.logger.Fatal("no metric family found for metric")
			}

			labelPairs := make([]*dto.LabelPair, 0, len(series[i].Labels))
			for j := range series[i].Labels {
				// Not using a for-loop value here because we need to store pointers to it,
				// which results in all labelPair containing the same values in the end.
				label := series[i].Labels[j]
				if label.Name == labels.MetricName {
					continue
				}
				labelPair := &dto.LabelPair{
					Name:  &label.Name,
					Value: &label.Value,
				}
				labelPairs = append(labelPairs, labelPair)
			}

			for j := range series[i].Samples {
				sample := series[i].Samples[j]
				metric := &dto.Metric{
					Label: labelPairs,
					Gauge: &dto.Gauge{
						Value: &sample.Value,
					},
					TimestampMs: &sample.Timestamp,
				}
				family.Metric = append(family.Metric, metric)
			}
		}
	}

	results := make([]*dto.MetricFamily, 0, len(families))
	for _, family := range families {
		results = append(results, family)
	}
	return results, nil
}
