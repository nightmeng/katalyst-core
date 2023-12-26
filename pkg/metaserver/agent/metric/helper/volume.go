package helper

import (
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"time"
)

func GetVolumeMetric(metricsFetcher metric.MetricsFetcher, emitter metrics.MetricEmitter, podUID, volumeName, metricName string, expireAt time.Time) (float64, error) {
	metricData, err := metricsFetcher.GetPodVolumeMetric(podUID, volumeName, metricName)
	if err != nil {
		general.Errorf(errMsgGetPodVolumeMetrics, podUID, volumeName, metricName, err)
		return 0, err
	}
	if !expireAt.IsZero() && metricData.Time.Before(expireAt) {
		general.Errorf(errMsgGetPodVolumeMetrics, podUID, volumeName, metricName, errMetricExpired)
		return 0, err
	}
	_ = emitter.StoreFloat64(metricsNamePodVolumeMetric, metricData.Value, metrics.MetricTypeNameRaw,
		metrics.ConvertMapToTags(map[string]string{
			metricsTagKeyPodUID:        podUID,
			metricsTagKeyPodVolumeName: volumeName,
			metricsTagKeyMetricName:    metricName,
		})...)
	return metricData.Value, nil
}
