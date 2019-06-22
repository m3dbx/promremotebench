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

package generators

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/devops"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
)

type HostsSimulator struct {
	hosts                 map[int][]*devops.Host
	scrapeIntervalSeconds int
	appendLabels          []*prompb.Label
}

type HostsSimulatorOptions struct {
	Labels map[string]string
}

func NewHostsSimulator(
	hostCount, scrapeIntervalSeconds int,
	start time.Time,
	opts HostsSimulatorOptions,
) *HostsSimulator {
	hosts := make(map[int][]*devops.Host, scrapeIntervalSeconds)

	for i := 0; i < scrapeIntervalSeconds; i++ {
		hosts[i] = make([]*devops.Host, 0, hostCount/scrapeIntervalSeconds+1)
	}

	for i := 0; i < hostCount; i++ {
		intervalOffsetSeconds := i % scrapeIntervalSeconds
		host := devops.NewHost(rand.Int(), rand.Int(), start.Add(time.Duration(intervalOffsetSeconds)*time.Second))
		hosts[intervalOffsetSeconds] = append(hosts[i%scrapeIntervalSeconds], &host)
	}

	var appendLabels []*prompb.Label
	if opts.Labels != nil {
		for k, v := range opts.Labels {
			appendLabels = append(appendLabels, &prompb.Label{
				Name:  k,
				Value: v,
			})
		}
	}

	return &HostsSimulator{
		hosts:                 hosts,
		scrapeIntervalSeconds: scrapeIntervalSeconds,
		appendLabels:          appendLabels,
	}
}

func (h *HostsSimulator) Hosts() []*devops.Host {
	var allHosts []*devops.Host
	for _, hosts := range h.hosts {
		allHosts = append(allHosts, hosts...)
	}

	return allHosts
}

func (h *HostsSimulator) Generate(offsetSeconds int, newSeriesPercent float64) []*prompb.TimeSeries {
	hosts := h.hosts[offsetSeconds]

	if hosts == nil {
		return nil
	}

	allSeries := make([]*prompb.TimeSeries, 0, len(hosts)*101)
	hostsToRemove := map[*devops.Host]struct{}{}

	for _, host := range hosts {
		allSeries = appendMeasurements(host, allSeries, h.appendLabels)
		if newSeriesPercent > 0 && rand.Float64()*100 < newSeriesPercent {
			hostsToRemove[host] = struct{}{}
			continue
		}

		host.TickAll(time.Duration(h.scrapeIntervalSeconds) * time.Second)
	}

	if len(hostsToRemove) > 0 {
		for i, host := range hosts {
			if _, exists := hostsToRemove[host]; exists {
				newHost := devops.NewHost(rand.Int(), rand.Int(), time.Now())
				newHost.TickAll((time.Duration(h.scrapeIntervalSeconds) * time.Second))
				hosts[i] = &newHost
			}
		}
	}

	return allSeries
}

func appendMeasurements(host *devops.Host, series []*prompb.TimeSeries, appendLabels []*prompb.Label) []*prompb.TimeSeries {
	for _, measurement := range host.SimulatedMeasurements {
		p := common.MakeUsablePoint()
		measurement.ToPoint(p)

		for i, fieldName := range p.FieldKeys {
			val := 0.0

			switch v := p.FieldValues[i].(type) {
			case int:
				val = float64(int(v))
			case int64:
				val = float64(int64(v))
			case float64:
				val = float64(v)
			default:
				panic(fmt.Sprintf("Cannot field %s with value type: %T with ", fieldName, v))
			}

			labels := []*prompb.Label{
				&prompb.Label{Name: string(devops.MachineTagKeys[0]), Value: string(host.Name)},
				&prompb.Label{Name: string(devops.MachineTagKeys[1]), Value: string(host.Region)},
				&prompb.Label{Name: string(devops.MachineTagKeys[2]), Value: string(host.Datacenter)},
				&prompb.Label{Name: string(devops.MachineTagKeys[3]), Value: string(host.Rack)},
				&prompb.Label{Name: string(devops.MachineTagKeys[4]), Value: string(host.OS)},
				&prompb.Label{Name: string(devops.MachineTagKeys[5]), Value: string(host.Arch)},
				&prompb.Label{Name: string(devops.MachineTagKeys[6]), Value: string(host.Team)},
				&prompb.Label{Name: string(devops.MachineTagKeys[7]), Value: string(host.Service)},
				&prompb.Label{Name: string(devops.MachineTagKeys[8]), Value: string(host.ServiceVersion)},
				&prompb.Label{Name: string(devops.MachineTagKeys[9]), Value: string(host.ServiceEnvironment)},
				&prompb.Label{Name: "measurement", Value: string(fieldName)},
				&prompb.Label{Name: labels.MetricName, Value: string(p.MeasurementName)},
			}

			if len(appendLabels) > 0 {
				labels = append(labels, appendLabels...)
			}

			sample := prompb.Sample{
				Value:     val,
				Timestamp: p.Timestamp.UnixNano() / int64(time.Millisecond),
			}

			series = append(series, &prompb.TimeSeries{
				Labels:  labels,
				Samples: []prompb.Sample{sample},
			})
		}
	}

	return series
}
