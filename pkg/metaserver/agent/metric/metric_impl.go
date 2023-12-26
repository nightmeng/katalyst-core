package metric

import (
	"context"
	"fmt"
	"github.com/kubewharf/katalyst-core/pkg/config"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/provisioner/kubelet"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/provisioner/malachite"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/types"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
	v1 "k8s.io/api/core/v1"
	"math/rand"
	"sync"
	"time"
)

type MetricsReaderImpl struct {
	metricStore *utilmetric.MetricStore
}

func NewMetricsReader(metricStore *utilmetric.MetricStore) types.MetricsReader {
	return &MetricsReaderImpl{metricStore: metricStore}
}

func (m *MetricsReaderImpl) GetNodeMetric(metricName string) (utilmetric.MetricData, error) {
	return m.metricStore.GetNodeMetric(metricName)
}

func (m *MetricsReaderImpl) GetNumaMetric(numaID int, metricName string) (utilmetric.MetricData, error) {
	return m.metricStore.GetNumaMetric(numaID, metricName)
}

func (m *MetricsReaderImpl) GetDeviceMetric(deviceName string, metricName string) (utilmetric.MetricData, error) {
	return m.metricStore.GetDeviceMetric(deviceName, metricName)
}

func (m *MetricsReaderImpl) GetCPUMetric(coreID int, metricName string) (utilmetric.MetricData, error) {
	return m.metricStore.GetCPUMetric(coreID, metricName)
}

func (m *MetricsReaderImpl) GetContainerMetric(podUID, containerName, metricName string) (utilmetric.MetricData, error) {
	return m.metricStore.GetContainerMetric(podUID, containerName, metricName)
}

func (m *MetricsReaderImpl) GetContainerNumaMetric(podUID, containerName, numaNode, metricName string) (utilmetric.MetricData, error) {
	return m.metricStore.GetContainerNumaMetric(podUID, containerName, numaNode, metricName)
}

func (m *MetricsReaderImpl) GetPodVolumeMetric(podUID, volumeName, metricName string) (utilmetric.MetricData, error) {
	return m.metricStore.GetPodVolumeMetric(podUID, volumeName, metricName)
}

func (m *MetricsReaderImpl) GetCgroupMetric(cgroupPath, metricName string) (utilmetric.MetricData, error) {
	return m.metricStore.GetCgroupMetric(cgroupPath, metricName)
}

func (m *MetricsReaderImpl) GetCgroupNumaMetric(cgroupPath, numaNode, metricName string) (utilmetric.MetricData, error) {
	return m.metricStore.GetCgroupNumaMetric(cgroupPath, numaNode, metricName)
}
func (m *MetricsReaderImpl) AggregatePodNumaMetric(podList []*v1.Pod, numaNode, metricName string,
	agg utilmetric.Aggregator, filter utilmetric.ContainerMetricFilter) utilmetric.MetricData {
	return m.metricStore.AggregatePodNumaMetric(podList, numaNode, metricName, agg, filter)
}

func (m *MetricsReaderImpl) AggregatePodMetric(podList []*v1.Pod, metricName string,
	agg utilmetric.Aggregator, filter utilmetric.ContainerMetricFilter) utilmetric.MetricData {
	return m.metricStore.AggregatePodMetric(podList, metricName, agg, filter)
}

func (m *MetricsReaderImpl) AggregateCoreMetric(cpuset machine.CPUSet, metricName string, agg utilmetric.Aggregator) utilmetric.MetricData {
	return m.metricStore.AggregateCoreMetric(cpuset, metricName, agg)
}

type MetricsNotifierManagerImpl struct {
	sync.RWMutex
	metricStore        *utilmetric.MetricStore
	registeredNotifier map[types.MetricsScope]map[string]types.NotifiedData
}

func NewMetricsNotifierManager(metricStore *utilmetric.MetricStore) types.MetricsNotifierManager {
	return &MetricsNotifierManagerImpl{
		metricStore: metricStore,
		registeredNotifier: map[types.MetricsScope]map[string]types.NotifiedData{
			types.MetricsScopeNode:      make(map[string]types.NotifiedData),
			types.MetricsScopeNuma:      make(map[string]types.NotifiedData),
			types.MetricsScopeCPU:       make(map[string]types.NotifiedData),
			types.MetricsScopeDevice:    make(map[string]types.NotifiedData),
			types.MetricsScopeContainer: make(map[string]types.NotifiedData),
		},
	}
}

func (m *MetricsNotifierManagerImpl) RegisterNotifier(scope types.MetricsScope, req types.NotifiedRequest,
	response chan types.NotifiedResponse) string {
	if _, ok := m.registeredNotifier[scope]; !ok {
		return ""
	}

	m.Lock()
	defer m.Unlock()

	randBytes := make([]byte, 30)
	rand.Read(randBytes)
	key := string(randBytes)

	m.registeredNotifier[scope][key] = types.NotifiedData{
		Scope:    scope,
		Req:      req,
		Response: response,
	}
	return key
}

func (m *MetricsNotifierManagerImpl) DeRegisterNotifier(scope types.MetricsScope, key string) {
	m.Lock()
	defer m.Unlock()

	delete(m.registeredNotifier[scope], key)
}

func (m *MetricsNotifierManagerImpl) Notify() {
	m.notifySystem()
	m.notifyPods()
}

// notifySystem notifies system-related data
func (m *MetricsNotifierManagerImpl) notifySystem() {
	now := time.Now()
	m.RLock()
	defer m.RUnlock()

	for _, reg := range m.registeredNotifier[types.MetricsScopeNode] {
		v, err := m.metricStore.GetNodeMetric(reg.Req.MetricName)
		if err != nil {
			continue
		} else if v.Time == nil {
			v.Time = &now
		}
		reg.Response <- types.NotifiedResponse{
			Req:        reg.Req,
			MetricData: v,
		}
	}

	for _, reg := range m.registeredNotifier[types.MetricsScopeDevice] {
		v, err := m.metricStore.GetDeviceMetric(reg.Req.DeviceID, reg.Req.MetricName)
		if err != nil {
			continue
		} else if v.Time == nil {
			v.Time = &now
		}
		reg.Response <- types.NotifiedResponse{
			Req:        reg.Req,
			MetricData: v,
		}
	}

	for _, reg := range m.registeredNotifier[types.MetricsScopeNuma] {
		v, err := m.metricStore.GetNumaMetric(reg.Req.NumaID, reg.Req.MetricName)
		if err != nil {
			continue
		} else if v.Time == nil {
			v.Time = &now
		}
		reg.Response <- types.NotifiedResponse{
			Req:        reg.Req,
			MetricData: v,
		}
	}

	for _, reg := range m.registeredNotifier[types.MetricsScopeCPU] {
		v, err := m.metricStore.GetCPUMetric(reg.Req.CoreID, reg.Req.MetricName)
		if err != nil {
			continue
		} else if v.Time == nil {
			v.Time = &now
		}
		reg.Response <- types.NotifiedResponse{
			Req:        reg.Req,
			MetricData: v,
		}
	}
}

// notifySystem notifies pod-related data
func (m *MetricsNotifierManagerImpl) notifyPods() {
	now := time.Now()
	m.RLock()
	defer m.RUnlock()

	for _, reg := range m.registeredNotifier[types.MetricsScopeContainer] {
		v, err := m.metricStore.GetContainerMetric(reg.Req.PodUID, reg.Req.ContainerName, reg.Req.MetricName)
		if err != nil {
			continue
		} else if v.Time == nil {
			v.Time = &now
		}
		reg.Response <- types.NotifiedResponse{
			Req:        reg.Req,
			MetricData: v,
		}

		if reg.Req.NumaID == 0 {
			continue
		}

		v, err = m.metricStore.GetContainerNumaMetric(reg.Req.PodUID, reg.Req.ContainerName, fmt.Sprintf("%v", reg.Req.NumaID), reg.Req.MetricName)
		if err != nil {
			continue
		} else if v.Time == nil {
			v.Time = &now
		}
		reg.Response <- types.NotifiedResponse{
			Req:        reg.Req,
			MetricData: v,
		}
	}
}

type ExternalMetricManagerImpl struct {
	sync.RWMutex
	metricStore      *utilmetric.MetricStore
	registeredMetric []func(store *utilmetric.MetricStore)
}

func NewExternalMetricManager(metricStore *utilmetric.MetricStore) types.ExternalMetricManager {
	return &ExternalMetricManagerImpl{
		metricStore: metricStore,
	}
}

func (m *ExternalMetricManagerImpl) RegisterExternalMetric(f func(store *utilmetric.MetricStore)) {
	m.Lock()
	defer m.Unlock()
	m.registeredMetric = append(m.registeredMetric, f)
}

func (m *ExternalMetricManagerImpl) Sample() {
	m.RLock()
	for _, f := range m.registeredMetric {
		f(m.metricStore)
	}
	m.RUnlock()
}

type MetricsFetcherImpl struct {
	types.MetricsReader
	types.MetricsNotifierManager
	types.ExternalMetricManager
	provisioners []types.MetricsProvisioner
}

func NewMetricsFetcher(emitter metrics.MetricEmitter, metaAgent *agent.MetaAgent, conf *config.Configuration) types.MetricsFetcher {
	metricStore := utilmetric.NewMetricStore()
	reader := NewMetricsReader(metricStore)
	metricsNotifierManager := NewMetricsNotifierManager(metricStore)
	externalMetricManager := NewExternalMetricManager(metricStore)

	malachiteProvisioner := malachite.NewMalachiteMetricsFetcher(metricStore, emitter, metaAgent, conf, metricsNotifierManager, externalMetricManager)
	kubeletProvisioner := kubelet.NewKubeletSummaryProvisioner(metricStore, emitter, conf)

	return &MetricsFetcherImpl{
		MetricsReader:          reader,
		MetricsNotifierManager: metricsNotifierManager,
		ExternalMetricManager:  externalMetricManager,
		provisioners:           []types.MetricsProvisioner{malachiteProvisioner, kubeletProvisioner},
	}
}

func (f *MetricsFetcherImpl) Run(ctx context.Context) {
	for _, provisioner := range f.provisioners {
		provisioner.Run(ctx)
	}
}

func (f *MetricsFetcherImpl) HasSynced() bool {
	for _, provisioner := range f.provisioners {
		if !provisioner.HasSynced() {
			return false
		}
	}
	return true
}
