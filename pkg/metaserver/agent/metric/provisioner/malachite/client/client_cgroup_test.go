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

package client

import (
	"encoding/json"
	types2 "github.com/kubewharf/katalyst-core/pkg/metaserver/agent/metric/provisioner/malachite/types"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kubewharf/katalyst-core/pkg/metaserver/agent/pod"
)

var subSystemGroupsV1Data []byte
var subSystemGroupsV2Data []byte

var (
	subSystemGroupsV1 = &types2.SubSystemGroupsV1{
		Memory: types2.MemoryCgV1{
			V1: struct {
				MemoryV1Data types2.MemoryCgDataV1 `json:"V1"`
			}(struct{ MemoryV1Data types2.MemoryCgDataV1 }{MemoryV1Data: types2.MemoryCgDataV1{}}),
		},
		Blkio: types2.BlkioCgV1{
			V1: struct {
				BlkIOData types2.BlkIOCgDataV1 `json:"V1"`
			}(struct{ BlkIOData types2.BlkIOCgDataV1 }{BlkIOData: types2.BlkIOCgDataV1{}}),
		},
		NetCls: types2.NetClsCg{
			NetData: types2.NetClsCgData{},
		},
		Cpuset: types2.CpusetCgV1{
			V1: struct {
				CPUSetData types2.CPUSetCgDataV1 `json:"V1"`
			}(struct{ CPUSetData types2.CPUSetCgDataV1 }{CPUSetData: types2.CPUSetCgDataV1{}}),
		},
		Cpuacct: types2.CpuacctCgV1{
			V1: struct {
				CPUData types2.CPUCgDataV1 `json:"V1"`
			}(struct{ CPUData types2.CPUCgDataV1 }{CPUData: types2.CPUCgDataV1{}}),
		},
	}

	subSystemGroupsV2 = &types2.SubSystemGroupsV2{
		Memory: types2.MemoryCgV2{
			V2: struct {
				MemoryData types2.MemoryCgDataV2 `json:"V2"`
			}(struct{ MemoryData types2.MemoryCgDataV2 }{MemoryData: types2.MemoryCgDataV2{}}),
		},
		Blkio: types2.BlkioCgV2{
			V2: struct {
				BlkIOData types2.BlkIOCgDataV2 `json:"V2"`
			}(struct{ BlkIOData types2.BlkIOCgDataV2 }{BlkIOData: types2.BlkIOCgDataV2{}}),
		},
		NetCls: types2.NetClsCg{
			NetData: types2.NetClsCgData{},
		},
		Cpuset: types2.CpusetCgV2{
			V2: struct {
				CPUSetData types2.CPUSetCgDataV2 `json:"V2"`
			}(struct{ CPUSetData types2.CPUSetCgDataV2 }{CPUSetData: types2.CPUSetCgDataV2{}}),
		},
		Cpuacct: types2.CpuacctCgV2{
			V2: struct {
				CPUData types2.CPUCgDataV2 `json:"V2"`
			}(struct{ CPUData types2.CPUCgDataV2 }{CPUData: types2.CPUCgDataV2{}}),
		},
	}
)

func init() {
	subSystemGroupsV1Data, _ = json.Marshal(subSystemGroupsV1)
	subSystemGroupsV2Data, _ = json.Marshal(subSystemGroupsV2)
}

func TestGetCgroupStats(t *testing.T) {
	t.Parallel()

	cgroupData := map[string]*types2.MalachiteCgroupResponse{
		"v1-path": {
			Status: 0,
			Data: types2.CgroupDataInner{
				CgroupType:      "V1",
				SubSystemGroups: subSystemGroupsV1Data,
			},
		},
		"v2-path": {
			Status: 0,
			Data: types2.CgroupDataInner{
				CgroupType:      "V2",
				SubSystemGroups: subSystemGroupsV2Data,
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Response == nil {
			r.Response = &http.Response{}
		}
		r.Response.StatusCode = http.StatusOK

		q := r.URL.Query()
		rPath := q.Get(CgroupPathParamKey)
		for path, info := range cgroupData {
			if path == rPath {
				data, _ := json.Marshal(info)
				_, _ = w.Write(data)
				return
			}
		}

		r.Response.StatusCode = http.StatusBadRequest
	}))
	defer server.Close()

	malachiteClient := NewMalachiteClient(&pod.PodFetcherStub{})
	malachiteClient.SetURL(map[string]string{
		CgroupResource: server.URL,
	})

	info, err := malachiteClient.GetCgroupStats("v1-path")
	assert.NoError(t, err)
	assert.NotNil(t, info.V1)
	assert.Nil(t, info.V2)

	info, err = malachiteClient.GetCgroupStats("v2-path")
	assert.NoError(t, err)
	assert.NotNil(t, info.V2)
	assert.Nil(t, info.V1)

	_, err = malachiteClient.GetCgroupStats("no-exist-path")
	assert.NotNil(t, err)
	assert.NotNil(t, info.V2)
	assert.Nil(t, info.V1)
}
