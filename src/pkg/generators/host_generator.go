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
	"sync"
	"time"

	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/devops"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/zap"
)

type HostsSimulator interface {
	Hosts() []devops.Host
	Generate(progressBy, scrapeDuration time.Duration, newSeriesPercent float64) (map[string][]prompb.TimeSeries, error)
}

type hostsSimulator struct {
	sync.RWMutex
	hosts        []devops.Host
	allHosts     []devops.Host
	appendLabels []prompb.Label
	hostIndex    int

	coldWritesPercent float64
	coldWritesRange   time.Duration
	logger            *zap.Logger
}

var _ HostsSimulator = (*hostsSimulator)(nil)

type HostsSimulatorOptions struct {
	Labels            map[string]string
	ColdWritesPercent float64
	ColdWritesRange   time.Duration
}

func NewHostsSimulator(
	hostCount int,
	start time.Time,
	opts HostsSimulatorOptions,
	logger *zap.Logger,
) *hostsSimulator {
	var hosts []devops.Host
	for i := 0; i < hostCount; i++ {
		host := devops.NewHost(i, 0, start)
		hosts = append(hosts, host)
	}

	var appendLabels []prompb.Label
	if opts.Labels != nil {
		for k, v := range opts.Labels {
			appendLabels = append(appendLabels, prompb.Label{
				Name:  k,
				Value: v,
			})
		}
	}

	return &hostsSimulator{
		hosts:        hosts,
		allHosts:     hosts,
		appendLabels: appendLabels,
		hostIndex:    hostCount,

		coldWritesPercent: opts.ColdWritesPercent,
		coldWritesRange:   opts.ColdWritesRange,
		logger:            logger,
	}
}

func (h *hostsSimulator) nextHostIndexWithLock() int {
	v := h.hostIndex
	h.hostIndex++
	return v
}

func (h *hostsSimulator) Hosts() []devops.Host {
	h.RLock()
	defer h.RUnlock()

	return append([]devops.Host{}, h.hosts...)
}

func (h *hostsSimulator) Generate(
	progressBy, scrapeDuration time.Duration,
	newSeriesPercent float64,
) (map[string][]prompb.TimeSeries, error) {
	h.Lock()
	defer h.Unlock()

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
			h.logger.Info("removing series",
				zap.Float64("newSeriesPercent", newSeriesPercent),
				zap.Int("len(h.allHosts)", len(h.allHosts)),
				zap.Int("remove", remove))
			h.allHosts = h.allHosts[:len(h.allHosts)-remove]
			for i := 0; i < remove; i++ {
				newHostIndex := h.nextHostIndexWithLock()
				newHost := devops.NewHost(newHostIndex, 0, now)
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

	hostValues := make(map[string][]prompb.TimeSeries)
	for _, host := range sendFromHosts {
		allSeries := make([]prompb.TimeSeries, 0, len(host.SimulatedMeasurements))
		for _, measurement := range host.SimulatedMeasurements {
			p := common.MakeUsablePoint()
			measurement.ToPoint(p)

			for i, fieldName := range p.FieldKeys {
				val := 0.0

				switch v := p.FieldValues[i].(type) {
				case int:
					val = float64(v)
				case int64:
					val = float64(v)
				case float64:
					val = v
				default:
					panic(fmt.Sprintf("bad field %s with value type: %T with ", fieldName, v))
				}

				labels := []prompb.Label{
					{Name: labels.MetricName, Value: string(p.MeasurementName)},
					{Name: "measurement", Value: string(fieldName)},
					{Name: string(devops.MachineTagKeys[0]), Value: string(host.Name)},
					{Name: string(devops.MachineTagKeys[1]), Value: string(host.Region)},
					{Name: string(devops.MachineTagKeys[2]), Value: string(host.Datacenter)},
					{Name: string(devops.MachineTagKeys[3]), Value: string(host.Rack)},
					{Name: string(devops.MachineTagKeys[4]), Value: string(host.OS)},
					{Name: string(devops.MachineTagKeys[5]), Value: string(host.Arch)},
					{Name: string(devops.MachineTagKeys[6]), Value: string(host.Team)},
					{Name: string(devops.MachineTagKeys[7]), Value: string(host.Service)},
					{Name: string(devops.MachineTagKeys[8]), Value: string(host.ServiceVersion)},
					{Name: string(devops.MachineTagKeys[9]), Value: string(host.ServiceEnvironment)},
				}
				if len(h.appendLabels) > 0 {
					labels = append(labels, h.appendLabels...)
				}

				timestamp := now
				if h.coldWritesPercent > 0 && rand.Float64() <= h.coldWritesPercent {
					timestamp = now.Add(-time.Duration(float64(h.coldWritesRange) * rand.Float64()))
				}

				timestampUnixMillis := timestamp.Truncate(30*time.Second).UnixNano() / int64(time.Millisecond)

				sample := prompb.Sample{
					Value:     val,
					Timestamp: timestampUnixMillis,
				}

				allSeries = append(allSeries, prompb.TimeSeries{
					Labels:  labels,
					Samples: []prompb.Sample{sample},
				})

			}
		}
		hostValues[string(host.Name)] = allSeries
	}

	return hostValues, nil
}
