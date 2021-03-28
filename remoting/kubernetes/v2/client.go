package v2

import (
	"context"
	"github.com/apache/dubbo-go/common/logger"
	perrors "github.com/pkg/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Client Kubernetes client-go client adapter
type Client struct {
	// manage the  client lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	Kc     kubernetes.Interface
}

// nolint
func (c *Client) Close() {
	select {
	case <-c.ctx.Done():
		//already stopped
		return
	default:
	}
	c.cancel()
}

// ValidateClient validates the kubernetes client
func ValidateClient(c *Client) error {
	if c.Kc == nil {
		kc, err := GetInClusterKubernetesClient()
		if err != nil {
			logger.Warnf("new kubernetes client: %v)", err)
			return perrors.WithMessage(err, "new kubernetes client")
		}

		c.Kc = kc
	}

	return nil
}

// GetInClusterKubernetesClient
// current pod running in kubernetes-cluster
func GetInClusterKubernetesClient() (kubernetes.Interface, error) {

	// read in-cluster config
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, perrors.WithMessage(err, "get in-cluster config")
	}

	return kubernetes.NewForConfig(cfg)
}
