package utils

import (
	"time"
	
	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
)

func GetMetricExpireTimestamp(dynamicConfig *dynamic.DynamicAgentConfiguration) time.Time {
	metricInsurancePeriod := dynamicConfig.GetDynamicConfiguration().MetricInsurancePeriod
	if metricInsurancePeriod == nil {
		return time.Time{}
	}

	return time.Now().Add(-*metricInsurancePeriod)
}
