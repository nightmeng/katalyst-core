package eviction

import (
	"fmt"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic/crd"
	"k8s.io/apimachinery/pkg/api/resource"
	evictionapi "k8s.io/kubernetes/pkg/kubelet/eviction/api"
	"strconv"
	"strings"
)

type RootfsPressureEvictionConfiguration struct {
	EnableRootfsPressureEviction         bool
	MinimumFreeThreshold                 *evictionapi.ThresholdValue
	MinimumInodesFreeThreshold           *evictionapi.ThresholdValue
	PodMinimumUsedThreshold              *evictionapi.ThresholdValue
	ReclaimedQoSPodUsedPriorityThreshold *evictionapi.ThresholdValue
	GracePeriod                          int64
}

func NewRootfsPressureEvictionPluginConfiguration() *RootfsPressureEvictionConfiguration {
	return &RootfsPressureEvictionConfiguration{}
}

func (c *RootfsPressureEvictionConfiguration) ApplyTo(conf *crd.DynamicConfigCRD) {
	if aqc := conf.AdminQoSConfiguration; aqc != nil && aqc.Spec.Config.EvictionConfig != nil &&
		aqc.Spec.Config.EvictionConfig.RootfsPressureEvictionConfig != nil {
		config := aqc.Spec.Config.EvictionConfig.RootfsPressureEvictionConfig
		if config.EnableRootfsPressureEviction != nil {
			c.EnableRootfsPressureEviction = *config.EnableRootfsPressureEviction
		}
		if config.MinimumFreeThreshold != "" {
			thresholdValue, err := parseThresholdValue(config.MinimumFreeThreshold)
			if err == nil {
				c.MinimumFreeThreshold = thresholdValue
			}
		}
		if config.MinimumInodesFreeThreshold != "" {
			thresholdValue, err := parseThresholdValue(config.MinimumInodesFreeThreshold)
			if err == nil {
				c.MinimumInodesFreeThreshold = thresholdValue
			}
		}
		if config.PodMinimumUsedThreshold != "" {
			thresholdValue, err := parseThresholdValue(config.PodMinimumUsedThreshold)
			if err == nil {
				c.PodMinimumUsedThreshold = thresholdValue
			}
		}
		if config.ReclaimedQoSPodUsedPriorityThreshold != "" {
			thresholdValue, err := parseThresholdValue(config.ReclaimedQoSPodUsedPriorityThreshold)
			if err == nil {
				c.ReclaimedQoSPodUsedPriorityThreshold = thresholdValue
			}
		}
		if config.GracePeriod != nil {
			c.GracePeriod = *config.GracePeriod
		}
	}
}

func parseThresholdValue(val string) (*evictionapi.ThresholdValue, error) {
	if strings.HasSuffix(val, "%") {
		// ignore 0% and 100%
		if val == "0%" || val == "100%" {
			return nil, nil
		}
		value, err := strconv.ParseFloat(strings.TrimRight(val, "%"), 32)
		if err != nil {
			return nil, err
		}
		percentage := float32(value) / 100
		if percentage < 0 {
			return nil, fmt.Errorf("eviction percentage threshold must be >= 0%%: %s", val)
		}
		if percentage > 100 {
			return nil, fmt.Errorf("eviction percentage threshold must be <= 100%%: %s", val)
		}
		return &evictionapi.ThresholdValue{
			Percentage: percentage,
		}, nil
	}
	quantity, err := resource.ParseQuantity(val)
	if err != nil {
		return nil, err
	}
	if quantity.Sign() < 0 || quantity.IsZero() {
		return nil, fmt.Errorf("eviction threshold must be positive: %s", &quantity)
	}
	return &evictionapi.ThresholdValue{
		Quantity: &quantity,
	}, nil
}
