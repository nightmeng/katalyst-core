/*
Copyright 2022 The Katalyst Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package eviction

import (
	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic/crd"
	"time"
)

type EvictionConfiguration struct {
	// Dryrun plugins is the list of plugins to dryrun
	// '*' means "all dryrun by default"
	// 'foo' means "dryrun 'foo'"
	// first item for a particular name wins
	DryRun                []string
	MetricInsurancePeriod *time.Duration

	*CPUPressureEvictionConfiguration
	*MemoryPressureEvictionConfiguration
	*RootfsPressureEvictionConfiguration
	*ReclaimedResourcesEvictionConfiguration
	*SystemLoadEvictionPluginConfiguration
}

func NewEvictionConfiguration() *EvictionConfiguration {
	return &EvictionConfiguration{
		CPUPressureEvictionConfiguration:        NewCPUPressureEvictionConfiguration(),
		MemoryPressureEvictionConfiguration:     NewMemoryPressureEvictionPluginConfiguration(),
		ReclaimedResourcesEvictionConfiguration: NewReclaimedResourcesEvictionConfiguration(),
		SystemLoadEvictionPluginConfiguration:   NewSystemLoadEvictionPluginConfiguration(),
	}
}

func (c *EvictionConfiguration) ApplyConfiguration(conf *crd.DynamicConfigCRD) {
	if aqc := conf.AdminQoSConfiguration; aqc != nil && aqc.Spec.Config.EvictionConfig != nil {
		if aqc.Spec.Config.EvictionConfig.DryRun != nil {
			c.DryRun = aqc.Spec.Config.EvictionConfig.DryRun
		}
		c.MetricInsurancePeriod = aqc.Spec.Config.EvictionConfig.MetricInsurancePeriod
	}

	c.CPUPressureEvictionConfiguration.ApplyConfiguration(conf)
	c.MemoryPressureEvictionConfiguration.ApplyConfiguration(conf)
	c.ReclaimedResourcesEvictionConfiguration.ApplyConfiguration(conf)
	c.SystemLoadEvictionPluginConfiguration.ApplyConfiguration(conf)
}
