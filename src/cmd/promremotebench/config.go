package main

import "github.com/m3db/m3/src/x/instrument"

// Configuration holds config options for the prom remote benchmarker.
type Configuration struct {
	Metrics instrument.MetricsConfiguration `yaml:"metrics"`
}
