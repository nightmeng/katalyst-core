package summary

import (
	"context"
	"github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/metrics"

	"k8s.io/klog/v2"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"

	"github.com/kubewharf/katalyst-core/pkg/config"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/external/summary/client"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
)

const (
	metricsNamKubeletSummaryUnHealthy = "kubelet_summary_unhealthy"
)

func ExternalMetricFunc(emitter metrics.MetricEmitter, conf *config.Configuration) func(store *utilmetric.MetricStore) {
	summaryClient := client.NewClient(conf)
	return func(store *utilmetric.MetricStore) {
		sample(context.TODO(), summaryClient, emitter, store)
	}
}

func sample(ctx context.Context, client *client.KubeletClient, emitter metrics.MetricEmitter, store *utilmetric.MetricStore) {
	summary, err := client.Summary(ctx)
	if err != nil {
		klog.Errorf("failed to update stats/summary from kubelet: %q", err)
		emitter.StoreInt64(metricsNamKubeletSummaryUnHealthy, 1, metrics.MetricTypeNameRaw)
		return
	}

	for _, podStats := range summary.Pods {
		for _, volumeStats := range podStats.VolumeStats {
			processVolumeStats(store, podStats.PodRef.UID, &volumeStats)
		}

		for _, containerStats := range podStats.Containers {
			processContainerRootfsStats(store, podStats.PodRef.UID, &containerStats)
			processContainerLogsStats(store, podStats.PodRef.UID, &containerStats)
		}

		// /etc/hosts
	}
}

func processVolumeStats(store *utilmetric.MetricStore, podUID string, volumeStats *statsapi.VolumeStats) {
	updateTime := volumeStats.Time.Time
	if volumeStats.AvailableBytes != nil {
		store.SetVolumeMetric(podUID, volumeStats.Name, consts.MetricsPodVolumeAvailable, utilmetric.MetricData{Value: float64(*volumeStats.AvailableBytes), Time: &updateTime})
	}
	if volumeStats.CapacityBytes != nil {
		store.SetVolumeMetric(podUID, volumeStats.Name, consts.MetricsPodVolumeCapacity, utilmetric.MetricData{Value: float64(*volumeStats.CapacityBytes), Time: &updateTime})
	}
	if volumeStats.Inodes != nil {
		store.SetVolumeMetric(podUID, volumeStats.Name, consts.MetricsPodVolumeInodes, utilmetric.MetricData{Value: float64(*volumeStats.Inodes), Time: &updateTime})
	}
	if volumeStats.InodesFree != nil {
		store.SetVolumeMetric(podUID, volumeStats.Name, consts.MetricsPodVolumeInodesFree, utilmetric.MetricData{Value: float64(*volumeStats.InodesFree), Time: &updateTime})
	}
	if volumeStats.InodesUsed != nil {
		store.SetVolumeMetric(podUID, volumeStats.Name, consts.MetricsPodVolumeInodesUsed, utilmetric.MetricData{Value: float64(*volumeStats.InodesUsed), Time: &updateTime})
	}
}

func processContainerRootfsStats(store *utilmetric.MetricStore, podUID string, containerStats *statsapi.ContainerStats) {
	updateTime := containerStats.Rootfs.Time.Time
	if containerStats.Rootfs.AvailableBytes != nil {
		store.SetContainerMetric(podUID, containerStats.Name, consts.MetricsRootfsAvailable, utilmetric.MetricData{Value: float64(*containerStats.Rootfs.AvailableBytes), Time: &updateTime})
	}
	if containerStats.Rootfs.CapacityBytes != nil {
		store.SetContainerMetric(podUID, containerStats.Name, consts.MetricsRootfsCapacity, utilmetric.MetricData{Value: float64(*containerStats.Rootfs.CapacityBytes), Time: &updateTime})
	}
	if containerStats.Rootfs.Inodes != nil {
		store.SetContainerMetric(podUID, containerStats.Name, consts.MetricsRootfsInodes, utilmetric.MetricData{Value: float64(*containerStats.Rootfs.Inodes), Time: &updateTime})
	}
	if containerStats.Rootfs.InodesFree != nil {
		store.SetContainerMetric(podUID, containerStats.Name, consts.MetricsRootfsInodesFree, utilmetric.MetricData{Value: float64(*containerStats.Rootfs.InodesFree), Time: &updateTime})
	}
	if containerStats.Rootfs.InodesUsed != nil {
		store.SetContainerMetric(podUID, containerStats.Name, consts.MetricsRootfsInodesUsed, utilmetric.MetricData{Value: float64(*containerStats.Rootfs.InodesUsed), Time: &updateTime})
	}
}

func processContainerLogsStats(store *utilmetric.MetricStore, podUID string, containerStats *statsapi.ContainerStats) {
	updateTime := containerStats.Logs.Time.Time
	if containerStats.Logs.AvailableBytes != nil {
		store.SetContainerMetric(podUID, containerStats.Name, consts.MetricsLogsAvailable, utilmetric.MetricData{Value: float64(*containerStats.Logs.AvailableBytes), Time: &updateTime})
	}
	if containerStats.Logs.CapacityBytes != nil {
		store.SetContainerMetric(podUID, containerStats.Name, consts.MetricsLogsCapacity, utilmetric.MetricData{Value: float64(*containerStats.Logs.CapacityBytes), Time: &updateTime})
	}
	if containerStats.Logs.Inodes != nil {
		store.SetContainerMetric(podUID, containerStats.Name, consts.MetricsLogsInodes, utilmetric.MetricData{Value: float64(*containerStats.Logs.Inodes), Time: &updateTime})
	}
	if containerStats.Logs.InodesFree != nil {
		store.SetContainerMetric(podUID, containerStats.Name, consts.MetricsLogsInodesFree, utilmetric.MetricData{Value: float64(*containerStats.Logs.InodesFree), Time: &updateTime})
	}
	if containerStats.Logs.InodesUsed != nil {
		store.SetContainerMetric(podUID, containerStats.Name, consts.MetricsLogsInodesUsed, utilmetric.MetricData{Value: float64(*containerStats.Logs.InodesUsed), Time: &updateTime})
	}
}
