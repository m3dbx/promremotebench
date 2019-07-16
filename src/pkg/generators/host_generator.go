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
	"math"
	"math/rand"
	"time"

	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/devops"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
)

type HostsSimulator struct {
	hosts        []devops.Host
	allHosts     []devops.Host
	appendLabels []*prompb.Label
}

type HostsSimulatorOptions struct {
	Labels map[string]string
}

func NewHostsSimulator(
	hostCount int,
	start time.Time,
	opts HostsSimulatorOptions,
) *HostsSimulator {
	var hosts []devops.Host
	for i := 0; i < hostCount; i++ {
		host := devops.NewHost(rand.Int(), 0, start)
		hosts = append(hosts, host)
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
		hosts:        hosts,
		allHosts:     hosts,
		appendLabels: appendLabels,
	}
}

func (h *HostsSimulator) Hosts() []devops.Host {
	return append([]devops.Host{}, h.hosts...)
}

func (h *HostsSimulator) Generate(
	progressBy, scrapeDuration time.Duration,
	newSeriesPercent float64,
) ([]*prompb.TimeSeries, error) {
	if newSeriesPercent < 0 || newSeriesPercent > 1 {
		return nil, fmt.Errorf(
			"newSeriesPercent not between [0.0,1.0]: value=%v",
			newSeriesPercent)
	}

	now := time.Now()
	factorProgress := float64(progressBy) / float64(scrapeDuration)
	numHosts := int(math.Ceil(factorProgress * float64(len(h.allHosts))))
	if numHosts == 0 {
		// Always progress by at least one
		numHosts = 1
	}
	if len(h.hosts) == 0 {
		// Out of hosts, remove/add hosts as needed and progress ticking
		for _, host := range h.allHosts {
			host.TickAll(progressBy)
		}
		if newSeriesPercent > 0 {
			remove := int(math.Ceil(newSeriesPercent * float64(len(h.allHosts))))
			h.allHosts = h.allHosts[:len(h.allHosts)-remove]
			for i := 0; i < remove; i++ {
				newHost := devops.NewHost(rand.Int(), 0, now)
				h.allHosts = append(h.allHosts, newHost)
			}
		}
		// Reset hosts
		h.hosts = h.allHosts
	}
	if len(h.hosts) < numHosts {
		numHosts = len(h.hosts)
	}

	// Select hosts
	sendFromHosts := h.hosts[:numHosts]

	// Progress hosts
	h.hosts = h.hosts[numHosts:]

	nowUnixMilliseconds := now.UnixNano() / int64(time.Millisecond)
	allSeries := make([]*prompb.TimeSeries, 0, len(sendFromHosts)*len(sendFromHosts[0].SimulatedMeasurements))
	for _, host := range sendFromHosts {
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
					panic(fmt.Sprintf("bad field %s with value type: %T with ", fieldName, v))
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
				if len(h.appendLabels) > 0 {
					labels = append(labels, h.appendLabels...)
				}

				sample := prompb.Sample{
					Value:     val,
					Timestamp: nowUnixMilliseconds,
				}

				allSeries = append(allSeries, &prompb.TimeSeries{
					Labels:  labels,
					Samples: []prompb.Sample{sample},
				})
			}
		}
	}

	return allSeries, nil
}
