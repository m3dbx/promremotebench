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
	"time"
	"math/rand"

	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/common"
	"github.com/influxdata/influxdb-comparisons/bulk_data_gen/devops"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/prompb"
)

type HostsSimulator struct {
	hosts                 []devops.Host
}

func NewHostsSimulator(hostCount int, start time.Time) *HostsSimulator {
	var hosts []devops.Host
	for i := 0; i < hostCount; i++ {
		host := devops.NewHost(rand.Int(), rand.Int(), start)
		hosts = append(hosts, host)
	}

	return &HostsSimulator{
		hosts: hosts,
	}
}

func (h *HostsSimulator) Generate(progressBy time.Duration) []*prompb.TimeSeries {
	nowUnixMilliseconds := time.Now().UnixNano() / int64(time.Millisecond)
	allSeries := make([]*prompb.TimeSeries, 0, len(h.hosts)*len(h.hosts[0].SimulatedMeasurements))
	for _, host := range h.hosts {
		if progressBy > 0 {
			host.TickAll(progressBy)
		}

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

	return allSeries
}
