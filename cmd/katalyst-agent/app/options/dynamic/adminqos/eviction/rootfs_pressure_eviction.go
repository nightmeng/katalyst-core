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
	"github.com/kubewharf/katalyst-core/pkg/config/agent/dynamic/adminqos/eviction"
	"github.com/pkg/errors"
	cliflag "k8s.io/component-base/cli/flag"
)

const (
	defaultEnableRootfsEviction                       = false
	defaultMinimumFreeThreshold                       = ""
	defaultMinimumInodesFreeThreshold                 = ""
	defaultPodMinimumUsedThreshold                    = ""
	defaultPodMinimumInodesUsedThreshold              = ""
	defaultReclaimedQoSPodUsedPriorityThreshold       = ""
	defaultReclaimedQoSPodInodesUsedPriorityThreshold = ""
)

type RootfsPressureEvictionOptions struct {
	EnableRootfsPressureEviction               bool
	MinimumNodeFsFreeThreshold                 string
	MinimumNodeFsInodesFreeThreshold           string
	MinimumImageFsFreeThreshold                string
	MinimumImageFsInodesFreeThreshold          string
	PodMinimumUsedThreshold                    string
	PodMinimumInodesUsedThreshold              string
	ReclaimedQoSPodUsedPriorityThreshold       string
	ReclaimedQoSPodInodesUsedPriorityThreshold string
	GracePeriod                                int64
}

func NewRootfsPressureEvictionOptions() *RootfsPressureEvictionOptions {
	return &RootfsPressureEvictionOptions{
		EnableRootfsPressureEviction:               defaultEnableRootfsEviction,
		MinimumNodeFsFreeThreshold:                 defaultMinimumFreeThreshold,
		MinimumNodeFsInodesFreeThreshold:           defaultMinimumInodesFreeThreshold,
		PodMinimumUsedThreshold:                    defaultPodMinimumUsedThreshold,
		PodMinimumInodesUsedThreshold:              defaultPodMinimumInodesUsedThreshold,
		ReclaimedQoSPodUsedPriorityThreshold:       defaultReclaimedQoSPodUsedPriorityThreshold,
		ReclaimedQoSPodInodesUsedPriorityThreshold: defaultReclaimedQoSPodInodesUsedPriorityThreshold,
		GracePeriod: defaultGracePeriod,
	}
}

func (o *RootfsPressureEvictionOptions) AddFlags(fss *cliflag.NamedFlagSets) {
	fs := fss.FlagSet("eviction-rootfs-pressure")
	fs.BoolVar(&o.EnableRootfsPressureEviction, "eviction-rootfs-enable", false,
		"set true to enable rootfs pressure eviction")
	fs.StringVar(&o.MinimumNodeFsFreeThreshold, "eviction-rootfs-minimum-nodefs-free", "",
		"the minimum node rootfs free threshold for nodes. once the node rootfs free space of current node is lower than this threshold, the eviction manager will try to evict some pods. example 200G, 10%")
	fs.StringVar(&o.MinimumNodeFsInodesFreeThreshold, "eviction-rootfs-minimum-nodefs-inodes-free", "",
		"the minimum node rootfs inodes free for nodes. once the node rootfs free inodes of current node is lower than this threshold, the eviction manager will try to evict some pods. example 20000, 10%")
	fs.StringVar(&o.MinimumImageFsFreeThreshold, "eviction-rootfs-minimum-imagefs-free", "",
		"the minimum image rootfs free threshold for nodes. once the image rootfs free space of current node is lower than this threshold, the eviction manager will try to evict some pods. example 200G, 10%")
	fs.StringVar(&o.MinimumImageFsInodesFreeThreshold, "eviction-rootfs-minimum-imagefs-inodes-free", "",
		"the minimum image rootfs inodes free for nodes. once the image rootfs free inodes of current node is lower than this threshold, the eviction manager will try to evict some pods. example 20000, 10%")
	fs.StringVar(&o.PodMinimumUsedThreshold, "eviction-rootfs-pod-minimum-used", "",
		"the minimum rootfs used for pod. the eviction manager will ignore this pod if its rootfs used in bytes is lower than this threshold. example 500M, 1%")
	fs.StringVar(&o.PodMinimumInodesUsedThreshold, "eviction-rootfs-pod-minimum-inodes-used", "",
		"the minimum rootfs inodes used for pod. the eviction manager will ignore this pod if its rootfs inodes used is lower than this threshold. example 2000, 1%")
	fs.StringVar(&o.ReclaimedQoSPodUsedPriorityThreshold, "eviction-rootfs-reclaimed-qos-pod-used-priority", "",
		"the rootfs used priority threshold for reclaimed qos pod. the eviction manager will prioritize the eviction of offline pods that reach this threshold. example 800M, 2%")
	fs.StringVar(&o.ReclaimedQoSPodInodesUsedPriorityThreshold, "eviction-rootfs-reclaimed-qos-pod-inodes-used-priority", "",
		"the rootfs inodes used priority threshold for reclaimed qos pod. the eviction manager will prioritize the eviction of reclaimed pods that reach this threshold. example 3000, 2%")
	fs.Int64Var(&o.GracePeriod, "eviction-rootfs-grace-period", 0,
		"the grace period of pod deletion")
}

func (o *RootfsPressureEvictionOptions) ApplyTo(c *eviction.RootfsPressureEvictionConfiguration) error {
	c.EnableRootfsPressureEviction = o.EnableRootfsPressureEviction
	if o.MinimumNodeFsFreeThreshold != "" {
		value, err := eviction.ParseThresholdValue(o.MinimumNodeFsFreeThreshold)
		if err != nil {
			return errors.Wrapf(err, "failed to parse option: 'eviction-rootfs-minimum-node-free'")
		}
		c.MinimumNodeFsFreeThreshold = value
	}
	if o.MinimumNodeFsInodesFreeThreshold != "" {
		value, err := eviction.ParseThresholdValue(o.MinimumNodeFsInodesFreeThreshold)
		if err != nil {
			return errors.Wrapf(err, "failed to parse option: 'eviction-rootfs-minimum-node-inodes-free'")
		}
		c.MinimumNodeFsInodesFreeThreshold = value
	}
	if o.MinimumImageFsFreeThreshold != "" {
		value, err := eviction.ParseThresholdValue(o.MinimumImageFsFreeThreshold)
		if err != nil {
			return errors.Wrapf(err, "failed to parse option: 'eviction-rootfs-minimum-image-free'")
		}
		c.MinimumImageFsFreeThreshold = value
	}
	if o.MinimumImageFsInodesFreeThreshold != "" {
		value, err := eviction.ParseThresholdValue(o.MinimumImageFsInodesFreeThreshold)
		if err != nil {
			return errors.Wrapf(err, "failed to parse option: 'eviction-rootfs-minimm-image-inodes-free'")
		}
		c.MinimumImageFsInodesFreeThreshold = value
	}
	if o.PodMinimumUsedThreshold != "" {
		value, err := eviction.ParseThresholdValue(o.PodMinimumUsedThreshold)
		if err != nil {
			return errors.Wrapf(err, "failed to parse option: 'eviction-rootfs-pod-minimum-used-threshold'")
		}
		c.PodMinimumUsedThreshold = value
	}
	if o.PodMinimumInodesUsedThreshold != "" {
		value, err := eviction.ParseThresholdValue(o.PodMinimumInodesUsedThreshold)
		if err != nil {
			return errors.Wrapf(err, "failed to parse option: 'eviction-rootfs-pod-minimum-inodes-used")
		}
		c.PodMinimumInodesUsedThreshold = value
	}
	if o.ReclaimedQoSPodUsedPriorityThreshold != "" {
		value, err := eviction.ParseThresholdValue(o.ReclaimedQoSPodUsedPriorityThreshold)
		if err != nil {
			return errors.Wrapf(err, "failed to parse option: 'eviction-rootfs-reclaimed-qos-pod-used-priority'")
		}
		c.ReclaimedQoSPodUsedPriorityThreshold = value
	}
	if o.ReclaimedQoSPodInodesUsedPriorityThreshold != "" {
		value, err := eviction.ParseThresholdValue(o.ReclaimedQoSPodInodesUsedPriorityThreshold)
		if err != nil {
			return errors.Wrapf(err, "failed to parse option: 'eviction-rootfs-reclaimed-qos-pod-inodes-used-priority'")
		}
		c.ReclaimedQoSPodInodesUsedPriorityThreshold = value
	}
	c.GracePeriod = o.GracePeriod
	return nil
}
