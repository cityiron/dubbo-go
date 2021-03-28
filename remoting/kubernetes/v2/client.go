package kubernetes

import (
    "context"
    perrors "github.com/pkg/errors"
    "k8s.io/client-go/kubernetes"
    "k8s.io/client-go/rest"
)

// Client Kubernetes client-go client adapter
type Client struct {
    // manage the  client lifecycle
    ctx    context.Context
    cancel context.CancelFunc
    kc kubernetes.Interface
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

func GetInClusterKubernetesClient() (kubernetes.Interface, error) {
    // read in-cluster config
    cfg, err := rest.InClusterConfig()
    if err != nil {
        return nil, perrors.WithMessage(err, "get in-cluster config")
    }

    return kubernetes.NewForConfig(cfg)
}


