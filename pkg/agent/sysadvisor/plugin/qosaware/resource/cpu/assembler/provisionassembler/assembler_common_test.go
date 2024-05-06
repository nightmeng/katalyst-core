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

package provisionassembler

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/kubewharf/katalyst-api/pkg/consts"
	katalyst_base "github.com/kubewharf/katalyst-core/cmd/base"
	"github.com/kubewharf/katalyst-core/cmd/katalyst-agent/app/options"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/metacache"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/plugin/qosaware/resource/cpu/region/provisionpolicy"
	"github.com/kubewharf/katalyst-core/pkg/agent/sysadvisor/types"
	"github.com/kubewharf/katalyst-core/pkg/metaserver"
	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric"
	"github.com/kubewharf/katalyst-core/pkg/metrics"
	metricspool "github.com/kubewharf/katalyst-core/pkg/metrics/metrics-pool"
	"github.com/kubewharf/katalyst-core/pkg/util/machine"
)

type FakeRegion struct {
	name                       string
	ownerPoolName              string
	regionType                 types.QoSRegionType
	bindingNumas               machine.CPUSet
	isNumaBinding              bool
	podSets                    types.PodSet
	controlKnob                types.ControlKnob
	headroom                   float64
	throttled                  bool
	provisionCurrentPolicyName types.CPUProvisionPolicyName
	provisionPolicyTopPriority types.CPUProvisionPolicyName
	headroomCurrentPolicyName  types.CPUHeadroomPolicyName
	headroomPolicyTopPriority  types.CPUHeadroomPolicyName
	controlEssentials          types.ControlEssentials
	essentials                 types.ResourceEssentials
}

func NewFakeRegion(name string, regionType types.QoSRegionType, ownerPoolName string) *FakeRegion {
	return &FakeRegion{
		name:          name,
		regionType:    regionType,
		ownerPoolName: ownerPoolName,
	}
}

func (fake *FakeRegion) Name() string {
	return fake.name
}
func (fake *FakeRegion) Type() types.QoSRegionType {
	return fake.regionType
}
func (fake *FakeRegion) OwnerPoolName() string {
	return fake.ownerPoolName
}
func (fake *FakeRegion) IsEmpty() bool {
	return false
}
func (fake *FakeRegion) Clear() {}
func (fake *FakeRegion) GetBindingNumas() machine.CPUSet {
	return fake.bindingNumas
}
func (fake *FakeRegion) SetPods(podSet types.PodSet) {
	fake.podSets = podSet
}
func (fake *FakeRegion) GetPods() types.PodSet {
	return fake.podSets
}
func (fake *FakeRegion) SetBindingNumas(bindingNumas machine.CPUSet) {
	fake.bindingNumas = bindingNumas
}
func (fake *FakeRegion) SetEssentials(essentials types.ResourceEssentials) {
	fake.essentials = essentials
}
func (fake *FakeRegion) SetIsNumaBinding(isNumaBinding bool) {
	fake.isNumaBinding = isNumaBinding
}
func (fake *FakeRegion) IsNumaBinding() bool {
	return fake.isNumaBinding
}
func (fake *FakeRegion) SetThrottled(throttled bool)                { fake.throttled = throttled }
func (fake *FakeRegion) AddContainer(ci *types.ContainerInfo) error { return nil }
func (fake *FakeRegion) TryUpdateProvision()                        {}
func (fake *FakeRegion) TryUpdateHeadroom()                         {}
func (fake *FakeRegion) UpdateStatus()                              {}
func (fake *FakeRegion) SetProvision(controlKnob types.ControlKnob) {
	fake.controlKnob = controlKnob
}
func (fake *FakeRegion) GetProvision() (types.ControlKnob, error) {
	return fake.controlKnob, nil
}
func (fake *FakeRegion) SetHeadroom(value float64) {
	fake.headroom = value
}
func (fake *FakeRegion) GetHeadroom() (float64, error) {
	return fake.headroom, nil
}
func (fake *FakeRegion) IsThrottled() bool {
	return fake.throttled
}
func (fake *FakeRegion) SetProvisionPolicy(policyTopPriority, currentPolicyName types.CPUProvisionPolicyName) {
	fake.provisionPolicyTopPriority = policyTopPriority
	fake.provisionCurrentPolicyName = currentPolicyName
}
func (fake *FakeRegion) GetProvisionPolicy() (types.CPUProvisionPolicyName, types.CPUProvisionPolicyName) {
	return fake.provisionPolicyTopPriority, fake.provisionCurrentPolicyName
}
func (fake *FakeRegion) SetHeadRoomPolicy(policyTopPriority, currentPolicyName types.CPUHeadroomPolicyName) {
	fake.headroomPolicyTopPriority = policyTopPriority
	fake.headroomCurrentPolicyName = currentPolicyName
}
func (fake *FakeRegion) GetHeadRoomPolicy() (types.CPUHeadroomPolicyName, types.CPUHeadroomPolicyName) {
	return fake.headroomPolicyTopPriority, fake.headroomCurrentPolicyName
}
func (fake *FakeRegion) GetStatus() types.RegionStatus {
	return types.RegionStatus{}
}
func (fake *FakeRegion) SetControlEssentials(controlEssentials types.ControlEssentials) {
	fake.controlEssentials = controlEssentials
}
func (fake *FakeRegion) GetControlEssentials() types.ControlEssentials {
	return fake.controlEssentials
}

func TestAssembleProvision(t *testing.T) {
	t.Parallel()

	provisionpolicy.RegisterInitializer(types.CPUProvisionPolicyCanonical, provisionpolicy.NewPolicyCanonical)

	conf, err := options.NewOptions().Config()
	require.NoError(t, err)
	require.NotNil(t, conf)

	stateFileDir := "stateFileDir"
	checkpointDir := "checkpointDir"

	conf.GenericSysAdvisorConfiguration.StateFileDirectory = stateFileDir
	conf.MetaServerConfiguration.CheckpointManagerDir = checkpointDir
	conf.CPUShareConfiguration.RestrictRefPolicy = nil
	conf.CPUAdvisorConfiguration.ProvisionPolicies = map[types.QoSRegionType][]types.CPUProvisionPolicyName{
		types.QoSRegionTypeShare: {types.CPUProvisionPolicyCanonical},
	}
	conf.GetDynamicConfiguration().EnableReclaim = true
	genericCtx, err := katalyst_base.GenerateFakeGenericContext([]runtime.Object{})
	require.NoError(t, err)

	metaServer, err := metaserver.NewMetaServer(genericCtx.Client, metrics.DummyMetrics{}, conf)
	require.NoError(t, err)
	defer func() {
		os.RemoveAll(stateFileDir)
		os.RemoveAll(checkpointDir)
	}()

	metaCache, err := metacache.NewMetaCacheImp(conf, metricspool.DummyMetricsEmitterPool{}, metric.NewFakeMetricsFetcher(metrics.DummyMetrics{}))
	require.NoError(t, err)
	ci := types.ContainerInfo{
		PodUID:              "pod1",
		ContainerName:       "container1",
		QoSLevel:            consts.PodAnnotationQoSLevelSharedCores,
		RegionNames:         sets.NewString("share-NUMA1"),
		OriginOwnerPoolName: "share-NUMA1",
		OwnerPoolName:       "share-NUMA1",
		TopologyAwareAssignments: map[int]machine.CPUSet{
			1: machine.NewCPUSet(1, 2, 3, 4, 5, 6, 7, 8),
		},
		OriginalTopologyAwareAssignments: map[int]machine.CPUSet{
			1: machine.NewCPUSet(1, 2, 3, 4, 5, 6, 7, 8),
		},
	}

	metaCache.SetContainerInfo("pod1", "container1", &ci)

	metaCache.SetPoolInfo("share-NUMA1", &types.PoolInfo{
		PoolName: "share-NUMA1",
		TopologyAwareAssignments: map[int]machine.CPUSet{
			1: machine.NewCPUSet(1, 2, 3, 4, 5, 6, 7, 8),
		},
		OriginalTopologyAwareAssignments: map[int]machine.CPUSet{
			1: machine.NewCPUSet(1, 2, 3, 4, 5, 6, 7, 8),
		},
		RegionNames: sets.NewString("share-NUMA1"),
	})

	metaCache.SetPoolInfo("share", &types.PoolInfo{
		PoolName: "share",
		TopologyAwareAssignments: map[int]machine.CPUSet{
			0: machine.NewCPUSet(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
		},
		OriginalTopologyAwareAssignments: map[int]machine.CPUSet{
			0: machine.NewCPUSet(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
		},
	})

	share := NewFakeRegion("share", types.QoSRegionTypeShare, "share")
	share.SetBindingNumas(machine.NewCPUSet(0))
	share.SetProvision(types.ControlKnob{
		types.ControlKnobNonReclaimedCPUSize: {Value: 6},
	})

	shareNumaBinding := NewFakeRegion("share-NUMA1", types.QoSRegionTypeShare, "share-NUMA1")
	shareNumaBinding.SetBindingNumas(machine.NewCPUSet(1))
	shareNumaBinding.SetIsNumaBinding(true)
	shareNumaBinding.SetProvision(types.ControlKnob{
		types.ControlKnobNonReclaimedCPUSize: {Value: 8},
	})
	// shareNumaBinding := region.NewQoSRegionShare(&ci, conf, nil, 1, metaCache, metaServer, metrics.DummyMetrics{})
	require.True(t, shareNumaBinding.IsNumaBinding(), "failed to check share region IsNumaBinding")

	shareNumaBinding.TryUpdateProvision()
	regionMap := map[string]region.QoSRegion{
		"share-NUMA1": shareNumaBinding,
		"share":       share,
	}

	reservedForReclaim := map[int]int{
		0: 4,
		1: 4,
	}

	numaAvailable := map[int]int{
		0: 20,
		1: 20,
	}

	nonBindingNumas := machine.NewCPUSet(0)

	common := NewProvisionAssemblerCommon(conf, nil, &regionMap, &reservedForReclaim, &numaAvailable, &nonBindingNumas, metaCache, metaServer, metrics.DummyMetrics{})
	result, err := common.AssembleProvision()
	require.NoErrorf(t, err, "failed to AssembleProvision: %s", err)
	require.NotNil(t, result, "invalid assembler result")
	t.Logf("%v", result)
	require.Equal(t, 6, result.PoolEntries["share"][-1], "invalid share non-reclaimed")
	require.Equal(t, 8, result.PoolEntries["share-NUMA1"][1], "invalid share-NUMA1 non-reclaimed")
	require.Equal(t, 18, result.PoolEntries["reclaim"][-1], "invalid share reclaimed")
	require.Equal(t, 16, result.PoolEntries["reclaim"][1], "invalid share-NUMA1 reclaimed")

	conf.GetDynamicConfiguration().EnableReclaim = false
	result, err = common.AssembleProvision()
	require.NoErrorf(t, err, "failed to AssembleProvision: %s", err)
	require.NotNil(t, result, "invalid assembler result")
	t.Logf("%v", result)
	require.Equal(t, 20, result.PoolEntries["share"][-1], "invalid share non-reclaimed")
	require.Equal(t, 20, result.PoolEntries["share-NUMA1"][1], "invalid share-NUMA1 non-reclaimed")

	isolationNumaBinding := NewFakeRegion("isolation-NUMA1", types.QoSRegionTypeIsolation, "isolation-NUMA1")
	isolationNumaBinding.SetBindingNumas(machine.NewCPUSet(1))
	isolationNumaBinding.SetIsNumaBinding(true)
	isolationNumaBinding.SetProvision(types.ControlKnob{
		types.ControlKnobNonReclaimedCPUSizeUpper: {Value: 8},
		types.ControlKnobNonReclaimedCPUSizeLower: {Value: 4},
	})
	require.True(t, isolationNumaBinding.IsNumaBinding(), "failed to check isolation region IsNumaBinding")

	metaCache.SetPoolInfo("isolation-NUMA1", &types.PoolInfo{
		PoolName: "isolation-NUMA1",
		TopologyAwareAssignments: map[int]machine.CPUSet{
			1: machine.NewCPUSet(20, 21, 22, 23),
		},
		OriginalTopologyAwareAssignments: map[int]machine.CPUSet{
			1: machine.NewCPUSet(20, 21, 22, 23),
		},
		RegionNames: sets.NewString("isolation-NUMA1"),
	})

	regionMap = map[string]region.QoSRegion{
		"share-NUMA1":     shareNumaBinding,
		"share":           share,
		"isolation-NUMA1": isolationNumaBinding,
	}

	conf.GetDynamicConfiguration().EnableReclaim = true
	common = NewProvisionAssemblerCommon(conf, nil, &regionMap, &reservedForReclaim, &numaAvailable, &nonBindingNumas, metaCache, metaServer, metrics.DummyMetrics{})
	result, err = common.AssembleProvision()
	require.NoErrorf(t, err, "failed to AssembleProvision: %s", err)
	require.NotNil(t, result, "invalid assembler result")
	t.Logf("%v", result)
	// reclaimed: 8, isolation: 8, share: 8
	require.Equal(t, 8, result.PoolEntries["share-NUMA1"][1], "invalid share-NUMA1 non-reclaimed")
	require.Equal(t, 8, result.PoolEntries["reclaim"][1], "invalid share-NUMA1 reclaimed")
	require.Equal(t, 8, result.PoolEntries["isolation-NUMA1"][1], "invalid isolation-NUMA1")
	require.Equal(t, 6, result.PoolEntries["share"][-1], "invalid share non-reclaimed")
	require.Equal(t, 18, result.PoolEntries["reclaim"][-1], "invalid share reclaimed")

	conf.GetDynamicConfiguration().EnableReclaim = false
	common = NewProvisionAssemblerCommon(conf, nil, &regionMap, &reservedForReclaim, &numaAvailable, &nonBindingNumas, metaCache, metaServer, metrics.DummyMetrics{})
	result, err = common.AssembleProvision()
	require.NoErrorf(t, err, "failed to AssembleProvision: %s", err)
	require.NotNil(t, result, "invalid assembler result")
	t.Logf("%v", result)
	// reclaimed: 4, isolation: 10, share: 10   the rest 4 cores is assigned to share and isolation pool
	require.Equal(t, 10, result.PoolEntries["share-NUMA1"][1], "invalid share-NUMA1 non-reclaimed")
	require.Equal(t, 4, result.PoolEntries["reclaim"][1], "invalid share-NUMA1 reclaimed")
	require.Equal(t, 10, result.PoolEntries["isolation-NUMA1"][1], "invalid isolation-NUMA1")
	require.Equal(t, 20, result.PoolEntries["share"][-1], "invalid share non-reclaimed")
	require.Equal(t, 4, result.PoolEntries["reclaim"][-1], "invalid share reclaimed")

	shareNumaBinding.SetProvision(types.ControlKnob{
		types.ControlKnobNonReclaimedCPUSize: {Value: 15},
	})
	shareNumaBinding.TryUpdateProvision()

	conf.GetDynamicConfiguration().EnableReclaim = false
	common = NewProvisionAssemblerCommon(conf, nil, &regionMap, &reservedForReclaim, &numaAvailable, &nonBindingNumas, metaCache, metaServer, metrics.DummyMetrics{})
	result, err = common.AssembleProvision()
	require.NoErrorf(t, err, "failed to AssembleProvision: %s", err)
	require.NotNil(t, result, "invalid assembler result")
	t.Logf("%v", result)
	// reclaimed: 4, isolation: 4, share: 16
	require.Equal(t, 16, result.PoolEntries["share-NUMA1"][1], "invalid share-NUMA1 non-reclaimed")
	require.Equal(t, 4, result.PoolEntries["reclaim"][1], "invalid share-NUMA1 reclaimed")
	require.Equal(t, 4, result.PoolEntries["isolation-NUMA1"][1], "invalid isolation-NUMA1")

	conf.GetDynamicConfiguration().EnableReclaim = true
	common = NewProvisionAssemblerCommon(conf, nil, &regionMap, &reservedForReclaim, &numaAvailable, &nonBindingNumas, metaCache, metaServer, metrics.DummyMetrics{})
	result, err = common.AssembleProvision()
	require.NoErrorf(t, err, "failed to AssembleProvision: %s", err)
	require.NotNil(t, result, "invalid assembler result")
	t.Logf("%v", result)
	// reclaimed: 5, isolation: 4, share: 15
	require.Equal(t, 15, result.PoolEntries["share-NUMA1"][1], "invalid share-NUMA1 non-reclaimed")
	require.Equal(t, 5, result.PoolEntries["reclaim"][1], "invalid share-NUMA1 reclaimed")
	require.Equal(t, 4, result.PoolEntries["isolation-NUMA1"][1], "invalid isolation-NUMA1")

	isolationNumaBinding2 := NewFakeRegion("isolation-NUMA1-pod2", types.QoSRegionTypeIsolation, "isolation-NUMA1-pod2")
	isolationNumaBinding2.SetBindingNumas(machine.NewCPUSet(1))
	isolationNumaBinding2.SetIsNumaBinding(true)
	isolationNumaBinding2.SetProvision(types.ControlKnob{
		types.ControlKnobNonReclaimedCPUSizeUpper: {Value: 8},
		types.ControlKnobNonReclaimedCPUSizeLower: {Value: 4},
	})
	require.True(t, isolationNumaBinding.IsNumaBinding(), "failed to check isolation region IsNumaBinding")

	metaCache.SetPoolInfo("isolation-NUMA1-pod2", &types.PoolInfo{
		PoolName: "isolation-NUMA1-pod2",
		TopologyAwareAssignments: map[int]machine.CPUSet{
			1: machine.NewCPUSet(20, 21, 22, 23),
		},
		OriginalTopologyAwareAssignments: map[int]machine.CPUSet{
			1: machine.NewCPUSet(20, 21, 22, 23),
		},
		RegionNames: sets.NewString("isolation-NUMA1-pod2"),
	})

	regionMap = map[string]region.QoSRegion{
		"share-NUMA1":          shareNumaBinding,
		"share":                share,
		"isolation-NUMA1":      isolationNumaBinding,
		"isolation-NUMA1-pod2": isolationNumaBinding2,
	}

	shareNumaBinding.SetProvision(types.ControlKnob{
		types.ControlKnobNonReclaimedCPUSize: {Value: 4},
	})
	shareNumaBinding.TryUpdateProvision()

	conf.GetDynamicConfiguration().EnableReclaim = true
	common = NewProvisionAssemblerCommon(conf, nil, &regionMap, &reservedForReclaim, &numaAvailable, &nonBindingNumas, metaCache, metaServer, metrics.DummyMetrics{})
	result, err = common.AssembleProvision()
	require.NoErrorf(t, err, "failed to AssembleProvision: %s", err)
	require.NotNil(t, result, "invalid assembler result")
	t.Logf("%v", result)
	// reclaimed: 4, isolation: 8/8, share: 4
	require.Equal(t, 4, result.PoolEntries["share-NUMA1"][1], "invalid share-NUMA1 non-reclaimed")
	// require.Equal(t, 4, result.PoolEntries["reclaim"][1], "invalid share-NUMA1 reclaimed")
	require.Equal(t, 8, result.PoolEntries["isolation-NUMA1"][1], "invalid isolation-NUMA1")
	require.Equal(t, 8, result.PoolEntries["isolation-NUMA1-pod2"][1], "invalid isolation-NUMA1-pod2")
	require.Equal(t, 4, result.PoolEntries["reclaim"][1], "invalid reclaim")

	shareNumaBinding.SetProvision(types.ControlKnob{
		types.ControlKnobNonReclaimedCPUSize: {Value: 8},
	})
	shareNumaBinding.TryUpdateProvision()
	conf.GetDynamicConfiguration().EnableReclaim = true
	common = NewProvisionAssemblerCommon(conf, nil, &regionMap, &reservedForReclaim, &numaAvailable, &nonBindingNumas, metaCache, metaServer, metrics.DummyMetrics{})
	result, err = common.AssembleProvision()
	require.NoErrorf(t, err, "failed to AssembleProvision: %s", err)
	require.NotNil(t, result, "invalid assembler result")
	t.Logf("%v", result)
	// reclaimed: 8 isolation: 4/4, share: 8
	require.Equal(t, 8, result.PoolEntries["share-NUMA1"][1], "invalid share-NUMA1 non-reclaimed")
	// require.Equal(t, 4, result.PoolEntries["reclaim"][1], "invalid share-NUMA1 reclaimed")
	require.Equal(t, 4, result.PoolEntries["isolation-NUMA1"][1], "invalid isolation-NUMA1")
	require.Equal(t, 4, result.PoolEntries["isolation-NUMA1-pod2"][1], "invalid isolation-NUMA1-pod2")
	require.Equal(t, 8, result.PoolEntries["reclaim"][1], "invalid reclaim")

	shareNumaBinding.SetProvision(types.ControlKnob{
		types.ControlKnobNonReclaimedCPUSize: {Value: 8},
	})
	shareNumaBinding.TryUpdateProvision()
	conf.GetDynamicConfiguration().EnableReclaim = false
	common = NewProvisionAssemblerCommon(conf, nil, &regionMap, &reservedForReclaim, &numaAvailable, &nonBindingNumas, metaCache, metaServer, metrics.DummyMetrics{})
	result, err = common.AssembleProvision()
	require.NoErrorf(t, err, "failed to AssembleProvision: %s", err)
	require.NotNil(t, result, "invalid assembler result")
	t.Logf("%v", result)
	// reclaimed: 4, isolation: 4/4, share: 12
	require.Equal(t, 10, result.PoolEntries["share-NUMA1"][1], "invalid share-NUMA1 non-reclaimed")
	// require.Equal(t, 4, result.PoolEntries["reclaim"][1], "invalid share-NUMA1 reclaimed")
	require.Equal(t, 5, result.PoolEntries["isolation-NUMA1"][1], "invalid isolation-NUMA1")
	require.Equal(t, 5, result.PoolEntries["isolation-NUMA1-pod2"][1], "invalid isolation-NUMA1-pod2")
	require.Equal(t, 4, result.PoolEntries["reclaim"][1], "invalid reclaim")
}
