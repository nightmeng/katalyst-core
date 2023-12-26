package disk

import (
	"context"
	"errors"
	"fmt"
	pluginapi "github.com/kubewharf/katalyst-api/pkg/protocol/evictionplugin/v1alpha1"
	"github.com/kubewharf/katalyst-core/pkg/agent/evictionmanager/plugin"
	"github.com/kubewharf/katalyst-core/pkg/agent/evictionmanager/plugin/utils"
	"github.com/kubewharf/katalyst-core/pkg/client"
	"github.com/kubewharf/katalyst-core/pkg/config"
	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic"
	"github.com/kubewharf/katalyst-core/pkg/config/generic"
	"github.com/kubewharf/katalyst-core/pkg/consts"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/helper"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/general"
	"github.com/kubewharf/katalyst-core/pkg/util/process"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/events"
	volumeutils "k8s.io/kubernetes/pkg/volume/util"
	"sort"
	"time"
)

const (
	EvictionPluginNamePodRootfsPressure = "rootfs-pressure-eviction-plugin"
	EvictionScopeSystemRootfs           = "SystemRootfs"
	evictionConditionSystemRootfs       = "SystemRootfs"
)

var (
	errMetricExpire = errors.New("metric expire")
)

type PodRootfsPressureEvictionPlugin struct {
	*process.StopControl
	pluginName    string
	dynamicConfig *dynamic.DynamicAgentConfiguration
	metaServer    *metaserver.MetaServer
	qosConf       *generic.QoSConfiguration
	emitter       metrics.MetricEmitter
}

func NewPodRootfsPressureEvictionPlugin(_ *client.GenericClientSet, _ events.EventRecorder,
	metaServer *metaserver.MetaServer, emitter metrics.MetricEmitter, conf *config.Configuration) plugin.EvictionPlugin {
	return &PodRootfsPressureEvictionPlugin{
		pluginName:    EvictionPluginNamePodRootfsPressure,
		metaServer:    metaServer,
		StopControl:   process.NewStopControl(time.Time{}),
		dynamicConfig: conf.DynamicAgentConfiguration,
		qosConf:       conf.GenericConfiguration.QoSConfiguration,
		emitter:       emitter,
	}
}

func (r *PodRootfsPressureEvictionPlugin) Name() string {
	if r == nil {
		return ""
	}
	return r.pluginName
}

func (r *PodRootfsPressureEvictionPlugin) Start() {
	return
}

func (r *PodRootfsPressureEvictionPlugin) ThresholdMet(_ context.Context) (*pluginapi.ThresholdMetResponse, error) {
	resp := &pluginapi.ThresholdMetResponse{
		MetType:       pluginapi.ThresholdMetType_NOT_MET,
		EvictionScope: EvictionScopeSystemRootfs,
	}

	config := r.dynamicConfig.GetDynamicConfiguration().RootfsPressureEvictionConfiguration
	if !config.EnableRootfsPressureEviction {
		return resp, nil
	}

	if config.MinimumFreeThreshold != nil {
		if config.MinimumFreeThreshold.Quantity != nil {
			// free <  config.MinimumFreeInBytesThreshold -> met
			systemFreeBytes, err := helper.GetNodeMetric(r.metaServer.MetricsFetcher, r.emitter, consts.MetricsSystemRootfsAvailable, utils.GetMetricExpireTimestamp(r.dynamicConfig))
			if err != nil {
				return nil, err
			}

			if int64(systemFreeBytes) > config.MinimumFreeThreshold.Quantity.Value() {
				return resp, nil
			}

			general.Infof("ThresholdMet result, Reason: MinimumFreeInBytesThreshold (Available: %d, Threshold: %d)", int64(systemFreeBytes), config.MinimumFreeThreshold.Quantity.Value())
			return &pluginapi.ThresholdMetResponse{
				MetType:       pluginapi.ThresholdMetType_HARD_MET,
				EvictionScope: EvictionScopeSystemRootfs,
				Condition: &pluginapi.Condition{
					ConditionType: pluginapi.ConditionType_NODE_CONDITION,
					Effects:       []string{string(v1.TaintEffectNoSchedule)},
					ConditionName: evictionConditionSystemRootfs,
					MetCondition:  true,
				},
			}, nil
		} else {
			// free/capacity < config.MinimumFreeRateThreshold -> met
			systemFreeBytes, err := helper.GetNodeMetric(r.metaServer.MetricsFetcher, r.emitter, consts.MetricsSystemRootfsAvailable, utils.GetMetricExpireTimestamp(r.dynamicConfig))
			if err != nil {
				return nil, err
			}
			systemCapacityBytes, err := helper.GetNodeMetric(r.metaServer.MetricsFetcher, r.emitter, consts.MetricsSystemRootfsCapacity, utils.GetMetricExpireTimestamp(r.dynamicConfig))
			if err != nil {
				return nil, err
			}

			if systemFreeBytes > systemCapacityBytes || systemCapacityBytes == 0 {
				return resp, nil
			}

			rate := systemFreeBytes / systemCapacityBytes
			if rate > float64(config.MinimumFreeThreshold.Percentage) {
				return resp, nil
			}

			general.Infof("ThresholdMet result, Reason: MinimumFreeRateThreshold (Rate: %02f, Threshold: %02f)", rate, config.MinimumFreeThreshold.Percentage)
			return &pluginapi.ThresholdMetResponse{
				MetType:       pluginapi.ThresholdMetType_HARD_MET,
				EvictionScope: EvictionScopeSystemRootfs,
				Condition: &pluginapi.Condition{
					ConditionType: pluginapi.ConditionType_NODE_CONDITION,
					Effects:       []string{string(v1.TaintEffectNoSchedule)},
					ConditionName: evictionConditionSystemRootfs,
					MetCondition:  true,
				},
			}, nil
		}
	}

	if config.MinimumInodesFreeThreshold != nil {
		if config.MinimumInodesFreeThreshold.Quantity != nil {
			systemInodesFree, err := helper.GetNodeMetric(r.metaServer.MetricsFetcher, r.emitter, consts.MetricsSystemRootfsInodesFree, utils.GetMetricExpireTimestamp(r.dynamicConfig))
			if err != nil {
				return nil, err
			}

			if int64(systemInodesFree) > config.MinimumInodesFreeThreshold.Quantity.Value() {
				return resp, nil
			}

			general.Infof("ThresholdMet result, Reason: MinimumInodesFreeThreshold (Free: %d, Threshold: %d)", int64(systemInodesFree), config.MinimumInodesFreeThreshold.Quantity.Value())
			return &pluginapi.ThresholdMetResponse{
				MetType:       pluginapi.ThresholdMetType_HARD_MET,
				EvictionScope: EvictionScopeSystemRootfs,
				Condition: &pluginapi.Condition{
					ConditionType: pluginapi.ConditionType_NODE_CONDITION,
					Effects:       []string{string(v1.TaintEffectNoSchedule)},
					ConditionName: evictionConditionSystemRootfs,
					MetCondition:  true,
				},
			}, nil
		} else {
			systemInodesFree, err := helper.GetNodeMetric(r.metaServer.MetricsFetcher, r.emitter, consts.MetricsSystemRootfsInodesFree, utils.GetMetricExpireTimestamp(r.dynamicConfig))
			if err != nil {
				return nil, err
			}
			systemInodes, err := helper.GetNodeMetric(r.metaServer.MetricsFetcher, r.emitter, consts.MetricsSystemRootfsInodes, utils.GetMetricExpireTimestamp(r.dynamicConfig))
			if err != nil {
				return nil, err
			}

			if systemInodesFree > systemInodes || systemInodes == 0 {
				return resp, nil
			}

			rate := systemInodesFree / systemInodes
			if rate > float64(config.MinimumInodesFreeThreshold.Percentage) {
				return resp, nil
			}

			general.Infof("ThresholdMet result, Reason: MinimumInodesFreeRateThreshold (Rate: %02f, Threshold: %02f)", rate, config.MinimumInodesFreeThreshold.Percentage)
			return &pluginapi.ThresholdMetResponse{
				MetType:       pluginapi.ThresholdMetType_HARD_MET,
				EvictionScope: EvictionScopeSystemRootfs,
				Condition: &pluginapi.Condition{
					ConditionType: pluginapi.ConditionType_NODE_CONDITION,
					Effects:       []string{string(v1.TaintEffectNoSchedule)},
					ConditionName: evictionConditionSystemRootfs,
					MetCondition:  true,
				},
			}, nil
		}
	}

	return resp, nil
}

func (r *PodRootfsPressureEvictionPlugin) GetTopEvictionPods(_ context.Context, request *pluginapi.GetTopEvictionPodsRequest) (*pluginapi.GetTopEvictionPodsResponse, error) {
	if request == nil {
		return nil, fmt.Errorf("GetTopEvictionPods got nil request")
	}

	if len(request.ActivePods) == 0 {
		general.Warningf("GetTopEvictionPods got empty active pods list")
		return &pluginapi.GetTopEvictionPodsResponse{}, nil
	}

	config := r.dynamicConfig.GetDynamicConfiguration().RootfsPressureEvictionConfiguration
	if !config.EnableRootfsPressureEviction {
		general.Warningf("GetTopEvictionPods RootfsPressureEviction is disabled")
		return &pluginapi.GetTopEvictionPodsResponse{}, nil
	}

	pods, err := r.getTopNPods(request.ActivePods, request.TopN)
	if err != nil {
		general.Warningf("GetTopEvictionPods get TopN pods failed: %q", err)
		return &pluginapi.GetTopEvictionPodsResponse{}, nil
	}

	resp := &pluginapi.GetTopEvictionPodsResponse{
		TargetPods: pods,
	}
	if gracePeriod := config.GracePeriod; gracePeriod > 0 {
		resp.DeletionOptions = &pluginapi.DeletionOptions{
			GracePeriodSeconds: gracePeriod,
		}
	}

	return resp, nil
}

func (r *PodRootfsPressureEvictionPlugin) GetEvictPods(_ context.Context, request *pluginapi.GetEvictPodsRequest) (*pluginapi.GetEvictPodsResponse, error) {
	if request == nil {
		return nil, fmt.Errorf("GetEvictPods got nil request")
	}

	return &pluginapi.GetEvictPodsResponse{}, nil
}

type podUsageItem struct {
	usage    int64
	priority bool
	pod      *v1.Pod
}

type podUsageList []podUsageItem

func (l podUsageList) Less(i, j int) bool {
	if l[i].priority && !l[j].priority {
		return true
	}
	if !l[i].priority && l[j].priority {
		return false
	}
	return l[i].usage > l[j].usage
}
func (l podUsageList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}
func (l podUsageList) Len() int {
	return len(l)
}

func (r *PodRootfsPressureEvictionPlugin) getTopNPods(pods []*v1.Pod, n uint64) ([]*v1.Pod, error) {
	var podList podUsageList

	config := r.dynamicConfig.GetDynamicConfiguration().RootfsPressureEvictionConfiguration
	minUsedThreshold := config.PodMinimumUsedThreshold
	offlinePriorityThreshold := config.ReclaimedQoSPodUsedPriorityThreshold
	for i := range pods {
		usage, capacity, err := r.getPodRootfsUsage(pods[i])
		if err != nil {
			return nil, err
		}

		percentage := float64(usage) / float64(capacity)
		if minUsedThreshold != nil {
			if minUsedThreshold.Quantity != nil && usage < minUsedThreshold.Quantity.Value() {
				continue
			} else {
				if percentage < float64(minUsedThreshold.Percentage) {
					continue
				}
			}
		}

		priority := false
		if offlinePriorityThreshold != nil && r.checkReclaimedQoSForPod(pods[i]) {
			if offlinePriorityThreshold.Quantity != nil && usage > offlinePriorityThreshold.Quantity.Value() {
				priority = true
			} else if percentage > float64(offlinePriorityThreshold.Percentage) {
				priority = true
			}
		}

		podList = append(podList, podUsageItem{
			usage:    usage,
			priority: priority,
			pod:      pods[i],
		})
	}

	if uint64(len(podList)) > n {
		sort.Sort(podList)
		podList = podList[:n]
	}

	var results []*v1.Pod
	for _, item := range podList {
		results = append(results, item.pod)
	}
	return results, nil
}

func (r *PodRootfsPressureEvictionPlugin) checkReclaimedQoSForPod(pod *v1.Pod) bool {
	if isReclaimed, err := r.qosConf.CheckReclaimedQoSForPod(pod); err != nil {
		general.Warningf("checkReclaimedQoSForPod: pod UID: %s, error: %q", pod.UID, err)
		return false
	} else {
		return isReclaimed
	}
}

func (r *PodRootfsPressureEvictionPlugin) getPodRootfsUsage(pod *v1.Pod) (int64, int64, error) {
	podUID := string(pod.UID)

	var usage int64

	for _, volume := range pod.Spec.Volumes {
		if !volumeutils.IsLocalEphemeralVolume(volume) {
			continue
		}

		volumeUsed, err := helper.GetVolumeMetric(r.metaServer.MetricsFetcher, r.emitter, podUID, volume.Name, consts.MetricsPodVolumeUsed, utils.GetMetricExpireTimestamp(r.dynamicConfig))
		if err != nil {
			return 0, 0, err
		}

		usage += int64(volumeUsed)
	}

	podRootfsUsed, err := helper.GetPodMetric(r.metaServer.MetricsFetcher, r.emitter, pod, consts.MetricsContainerRootfsUsed, -1, utils.GetMetricExpireTimestamp(r.dynamicConfig))
	if err != nil {
		return 0, 0, err
	}
	usage += int64(podRootfsUsed)

	rootfsCapacity, err := r.metaServer.GetNodeMetric(consts.MetricsSystemRootfsCapacity)
	if err != nil {
		return 0, 0, err
	}

	// TODO /etc/hosts

	return usage, int64(rootfsCapacity.Value), nil
}
