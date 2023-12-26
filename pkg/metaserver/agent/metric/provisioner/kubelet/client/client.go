package client

import (
	"context"
	"fmt"
	"github.com/kubewharf/katalyst-core/pkg/config"
	"github.com/kubewharf/katalyst-core/pkg/util/native"
	"github.com/kubewharf/katalyst-core/pkg/util/process"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

type KubeletClient struct {
	conf *config.Configuration
}

// http://127.0.0.1:10255
func NewClient(conf *config.Configuration) *KubeletClient {
	return &KubeletClient{
		conf: conf,
	}
}

func (c *KubeletClient) Summary(ctx context.Context) (*statsapi.Summary, error) {
	summary := &statsapi.Summary{}
	if c.conf.EnableKubeletSecurePort {
		if err := native.GetAndUnmarshalForHttps(ctx, c.conf.KubeletSecurePort, c.conf.NodeAddress, c.conf.KubeletPodsEndpoint, c.conf.APIAuthTokenFile, summary); err != nil {
			return nil, fmt.Errorf("failed to get kubelet config for summary api, error: %v", err)
		}
	} else {
		const podsApi = "http://localhost:10255/stats/summary"
		if err := process.GetAndUnmarshal(podsApi, summary); err != nil {
			return nil, fmt.Errorf("failed to get summary, error: %v", err)
		}
	}

	return summary, nil
}
