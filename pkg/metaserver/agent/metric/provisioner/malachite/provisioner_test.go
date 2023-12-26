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

package malachite

import (
	types2 "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/provisioner/malachite/types"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/types"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	metric2 "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
	utilmetric "github.com/kubewharf/katalyst-core/pkg/util/metric"
)

func Test_noneExistMetricsProvisioner(t *testing.T) {
	t.Parallel()

	store := utilmetric.NewMetricStore()
	reader := metric2.NewMetricsReader(store)

	var err error
	implement := NewMalachiteMetricsFetcher(store, metrics.DummyMetrics{}, &pod.PodFetcherStub{}, nil)

	fakeSystemCompute := &types2.SystemComputeData{
		CPU: []types2.CPU{
			{
				Name: "CPU1111",
			},
		},
	}
	fakeSystemMemory := &types2.SystemMemoryData{
		Numa: []types2.Numa{
			{},
		},
	}
	fakeSystemIO := &types2.SystemDiskIoData{
		DiskIo: []types2.DiskIo{
			{},
		},
	}
	fakeCgroupInfoV1 := &types2.MalachiteCgroupInfo{
		CgroupType: "V1",
		V1: &types2.MalachiteCgroupV1Info{
			Memory: &types2.MemoryCgDataV1{},
			Blkio:  &types2.BlkIOCgDataV1{},
			NetCls: &types2.NetClsCgData{},
			CpuSet: &types2.CPUSetCgDataV1{},
			Cpu:    &types2.CPUCgDataV1{},
		},
	}
	fakeCgroupInfoV2 := &types2.MalachiteCgroupInfo{
		CgroupType: "V2",
		V2: &types2.MalachiteCgroupV2Info{
			Memory: &types2.MemoryCgDataV2{},
			Blkio:  &types2.BlkIOCgDataV2{},
			NetCls: &types2.NetClsCgData{},
			CpuSet: &types2.CPUSetCgDataV2{},
			Cpu:    &types2.CPUCgDataV2{},
		},
	}

	implement.(*MalachiteMetricsProvisioner).processSystemComputeData(fakeSystemCompute)
	implement.(*MalachiteMetricsProvisioner).processSystemMemoryData(fakeSystemMemory)
	implement.(*MalachiteMetricsProvisioner).processSystemIOData(fakeSystemIO)
	implement.(*MalachiteMetricsProvisioner).processSystemNumaData(fakeSystemMemory)
	implement.(*MalachiteMetricsProvisioner).processSystemCPUComputeData(fakeSystemCompute)

	implement.(*MalachiteMetricsProvisioner).processContainerCPUData("pod-not-exist", "container-not-exist", fakeCgroupInfoV1)
	implement.(*MalachiteMetricsProvisioner).processContainerMemoryData("pod-not-exist", "container-not-exist", fakeCgroupInfoV1)
	implement.(*MalachiteMetricsProvisioner).processContainerBlkIOData("pod-not-exist", "container-not-exist", fakeCgroupInfoV1)
	implement.(*MalachiteMetricsProvisioner).processContainerNetData("pod-not-exist", "container-not-exist", fakeCgroupInfoV1)
	implement.(*MalachiteMetricsProvisioner).processContainerPerfData("pod-not-exist", "container-not-exist", fakeCgroupInfoV1)
	implement.(*MalachiteMetricsProvisioner).processContainerPerNumaMemoryData("pod-not-exist", "container-not-exist", fakeCgroupInfoV1)

	implement.(*MalachiteMetricsProvisioner).processContainerCPUData("pod-not-exist", "container-not-exist", fakeCgroupInfoV2)
	implement.(*MalachiteMetricsProvisioner).processContainerMemoryData("pod-not-exist", "container-not-exist", fakeCgroupInfoV2)
	implement.(*MalachiteMetricsProvisioner).processContainerBlkIOData("pod-not-exist", "container-not-exist", fakeCgroupInfoV2)
	implement.(*MalachiteMetricsProvisioner).processContainerNetData("pod-not-exist", "container-not-exist", fakeCgroupInfoV2)
	implement.(*MalachiteMetricsProvisioner).processContainerPerfData("pod-not-exist", "container-not-exist", fakeCgroupInfoV2)
	implement.(*MalachiteMetricsProvisioner).processContainerPerNumaMemoryData("pod-not-exist", "container-not-exist", fakeCgroupInfoV2)

	_, err = reader.GetNodeMetric("test-not-exist")
	if err == nil {
		t.Errorf("GetNode() error = %v, wantErr not nil", err)
		return
	}

	_, err = reader.GetNumaMetric(1, "test-not-exist")
	if err == nil {
		t.Errorf("GetNode() error = %v, wantErr not nil", err)
		return
	}

	_, err = reader.GetDeviceMetric("device-not-exist", "test-not-exist")
	if err == nil {
		t.Errorf("GetNode() error = %v, wantErr not nil", err)
		return
	}

	_, err = reader.GetCPUMetric(1, "test-not-exist")
	if err == nil {
		t.Errorf("GetNode() error = %v, wantErr not nil", err)
		return
	}

	_, err = reader.GetContainerMetric("pod-not-exist", "container-not-exist", "test-not-exist")
	if err == nil {
		t.Errorf("GetNode() error = %v, wantErr not nil", err)
		return
	}

	_, err = reader.GetContainerNumaMetric("pod-not-exist", "container-not-exist", "", "test-not-exist")
	if err == nil {
		t.Errorf("GetContainerNuma() error = %v, wantErr not nil", err)
		return
	}
}

func Test_notifySystem(t *testing.T) {
	t.Parallel()

	now := time.Now()

	store := utilmetric.NewMetricStore()
	notifierManager := metric2.NewMetricsNotifierManager(store)

	f := NewMalachiteMetricsFetcher(store, metrics.DummyMetrics{}, &pod.PodFetcherStub{}, nil)

	rChan := make(chan types.NotifiedResponse, 20)
	notifierManager.RegisterNotifier(types.MetricsScopeNode, types.NotifiedRequest{
		MetricName: "test-node-metric",
	}, rChan)
	notifierManager.RegisterNotifier(types.MetricsScopeNuma, types.NotifiedRequest{
		MetricName: "test-numa-metric",
		NumaID:     1,
	}, rChan)
	notifierManager.RegisterNotifier(types.MetricsScopeCPU, types.NotifiedRequest{
		MetricName: "test-cpu-metric",
		CoreID:     2,
	}, rChan)
	notifierManager.RegisterNotifier(types.MetricsScopeDevice, types.NotifiedRequest{
		MetricName: "test-device-metric",
		DeviceID:   "test-device",
	}, rChan)
	notifierManager.RegisterNotifier(types.MetricsScopeContainer, types.NotifiedRequest{
		MetricName:    "test-container-metric",
		PodUID:        "test-pod",
		ContainerName: "test-container",
	}, rChan)
	notifierManager.RegisterNotifier(types.MetricsScopeContainer, types.NotifiedRequest{
		MetricName:    "test-container-numa-metric",
		PodUID:        "test-pod",
		ContainerName: "test-container",
		NumaNode:      "3",
	}, rChan)

	m := f.(*MalachiteMetricsProvisioner)
	m.metricStore.SetNodeMetric("test-node-metric", utilmetric.MetricData{Value: 34, Time: &now})
	m.metricStore.SetNumaMetric(1, "test-numa-metric", utilmetric.MetricData{Value: 56, Time: &now})
	m.metricStore.SetCPUMetric(2, "test-cpu-metric", utilmetric.MetricData{Value: 78, Time: &now})
	m.metricStore.SetDeviceMetric("test-device", "test-device-metric", utilmetric.MetricData{Value: 91, Time: &now})
	m.metricStore.SetContainerMetric("test-pod", "test-container", "test-container-metric", utilmetric.MetricData{Value: 91, Time: &now})
	m.metricStore.SetContainerNumaMetric("test-pod", "test-container", "3", "test-container-numa-metric", utilmetric.MetricData{Value: 75, Time: &now})

	go func() {
		for {
			select {
			case response := <-rChan:
				switch response.Req.MetricName {
				case "test-node-metric":
					assert.Equal(t, response.Value, 34)
				case "test-numa-metric":
					assert.Equal(t, response.Value, 56)
				case "test-cpu-metric":
					assert.Equal(t, response.Value, 78)
				case "test-device-metric":
					assert.Equal(t, response.Value, 91)
				case "test-container-metric":
					assert.Equal(t, response.Value, 91)
				case "test-container-numa-metric":
					assert.Equal(t, response.Value, 75)
				}
			}
		}
	}()

	time.Sleep(time.Millisecond * 3)
}

func TestStore_Aggregate(t *testing.T) {
	t.Parallel()

	now := time.Now()

	store := utilmetric.NewMetricStore()
	reader := metric2.NewMetricsReader(store)

	f := NewMalachiteMetricsFetcher(store, metrics.DummyMetrics{}, &pod.PodFetcherStub{}, nil).(*MalachiteMetricsProvisioner)

	pod1 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: "pod1",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "container1",
				},
				{
					Name: "container2",
				},
			},
		},
	}
	pod2 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: "pod2",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "container3",
				},
			},
		},
	}
	pod3 := &v1.Pod{}

	f.metricStore.SetContainerMetric("pod1", "container1", "test-pod-metric", utilmetric.MetricData{Value: 1, Time: &now})
	f.metricStore.SetContainerMetric("pod1", "container2", "test-pod-metric", utilmetric.MetricData{Value: 1, Time: &now})
	f.metricStore.SetContainerMetric("pod2", "container3", "test-pod-metric", utilmetric.MetricData{Value: 1, Time: &now})
	sum := reader.AggregatePodMetric([]*v1.Pod{pod1, pod2, pod3}, "test-pod-metric", utilmetric.AggregatorSum, utilmetric.DefaultContainerMetricFilter)
	assert.Equal(t, float64(3), sum.Value)
	avg := reader.AggregatePodMetric([]*v1.Pod{pod1, pod2, pod3}, "test-pod-metric", utilmetric.AggregatorAvg, utilmetric.DefaultContainerMetricFilter)
	assert.Equal(t, float64(1.5), avg.Value)

	f.metricStore.SetContainerNumaMetric("pod1", "container1", "0", "test-pod-numa-metric", utilmetric.MetricData{Value: 1, Time: &now})
	f.metricStore.SetContainerNumaMetric("pod1", "container2", "0", "test-pod-numa-metric", utilmetric.MetricData{Value: 1, Time: &now})
	f.metricStore.SetContainerNumaMetric("pod1", "container2", "1", "test-pod-numa-metric", utilmetric.MetricData{Value: 1, Time: &now})
	f.metricStore.SetContainerNumaMetric("pod2", "container3", "0", "test-pod-numa-metric", utilmetric.MetricData{Value: 1, Time: &now})
	f.metricStore.SetContainerNumaMetric("pod2", "container3", "1", "test-pod-numa-metric", utilmetric.MetricData{Value: 1, Time: &now})
	f.metricStore.SetContainerNumaMetric("pod2", "container3", "1", "test-pod-numa-metric", utilmetric.MetricData{Value: 1, Time: &now})
	sum = reader.AggregatePodNumaMetric([]*v1.Pod{pod1, pod2, pod3}, "0", "test-pod-numa-metric", utilmetric.AggregatorSum, utilmetric.DefaultContainerMetricFilter)
	assert.Equal(t, float64(3), sum.Value)
	avg = reader.AggregatePodNumaMetric([]*v1.Pod{pod1, pod2, pod3}, "0", "test-pod-numa-metric", utilmetric.AggregatorAvg, utilmetric.DefaultContainerMetricFilter)
	assert.Equal(t, float64(1.5), avg.Value)
	sum = reader.AggregatePodNumaMetric([]*v1.Pod{pod1, pod2, pod3}, "1", "test-pod-numa-metric", utilmetric.AggregatorSum, utilmetric.DefaultContainerMetricFilter)
	assert.Equal(t, float64(2), sum.Value)
	avg = reader.AggregatePodNumaMetric([]*v1.Pod{pod1, pod2, pod3}, "1", "test-pod-numa-metric", utilmetric.AggregatorAvg, utilmetric.DefaultContainerMetricFilter)
	assert.Equal(t, float64(1), avg.Value)

	f.metricStore.SetCPUMetric(1, "test-cpu-metric", utilmetric.MetricData{Value: 1, Time: &now})
	f.metricStore.SetCPUMetric(1, "test-cpu-metric", utilmetric.MetricData{Value: 2, Time: &now})
	f.metricStore.SetCPUMetric(2, "test-cpu-metric", utilmetric.MetricData{Value: 1, Time: &now})
	f.metricStore.SetCPUMetric(0, "test-cpu-metric", utilmetric.MetricData{Value: 1, Time: &now})
	sum = reader.AggregateCoreMetric(machine.NewCPUSet(0, 1, 2, 3), "test-cpu-metric", utilmetric.AggregatorSum)
	assert.Equal(t, float64(4), sum.Value)
	avg = reader.AggregateCoreMetric(machine.NewCPUSet(0, 1, 2, 3), "test-cpu-metric", utilmetric.AggregatorAvg)
	assert.Equal(t, float64(4/3.), avg.Value)
}
