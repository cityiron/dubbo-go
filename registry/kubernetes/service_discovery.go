package kubernetes

import (
	"encoding/json"
	"fmt"
	"github.com/apache/dubbo-go/common"
	"github.com/apache/dubbo-go/common/constant"
	"github.com/apache/dubbo-go/common/extension"
	"github.com/apache/dubbo-go/common/logger"
	"github.com/apache/dubbo-go/config"
	"github.com/apache/dubbo-go/registry"
	"github.com/apache/dubbo-go/remoting/kubernetes"
	gxset "github.com/dubbogo/gost/container/set"
	gxpage "github.com/dubbogo/gost/hash/page"
	perrors "github.com/pkg/errors"
	"os"
	"sync"
)

const (
	// kubernetesPropertiesKey Stay in line with dubbo
	kubernetesPropertiesKey = "io.dubbo/metadata"
)

var (
	// 16 would be enough. We won't use concurrentMap because in most cases, there are not race condition
	instanceMap = make(map[string]registry.ServiceDiscovery, 16)
	initLock    sync.Mutex
)

// init will put the service discovery into extension
func init() {
	extension.SetServiceDiscovery(constant.KUBERNETES_KEY, newKubernetesServiceDiscovery)
}

type kubernetesServiceDiscovery struct {
	currentHostname      string
	localServiceInstance registry.ServiceInstance
	namespace            string
	enableRegister       bool
	client               kubernetes.Client
	url                  *common.URL
}

// newKubernetesServiceDiscovery will create new service discovery instance
// use double-check pattern to reduce race condition
func newKubernetesServiceDiscovery(name string) (registry.ServiceDiscovery, error) {
	instance, ok := instanceMap[name]
	if ok {
		return instance, nil
	}

	initLock.Lock()
	defer initLock.Unlock()

	// double check
	instance, ok = instanceMap[name]
	if ok {
		return instance, nil
	}

	chn := os.Getenv("HOSTNAME")

	sdc, ok := config.GetBaseConfig().GetServiceDiscoveries(name)
	if !ok || len(sdc.RemoteRef) == 0 {
		return nil, perrors.New("could not init the instance because the config is invalid")
	}
	remoteConfig, ok := config.GetBaseConfig().GetRemoteConfig(sdc.RemoteRef)
	if !ok {
		return nil, perrors.New("could not find the remote config for name: " + sdc.RemoteRef)
	}

	return &kubernetesServiceDiscovery{

	}, nil
}

func (sd *kubernetesServiceDiscovery) String() string {
	return fmt.Sprintf("zookeeper-service-discovery[%s]", sd.url)
}

func (sd *kubernetesServiceDiscovery) Destroy() error {
	sd.client.Close()
	return nil
}

func (sd *kubernetesServiceDiscovery) Register(instance registry.ServiceInstance) error {
	sd.localServiceInstance = instance

	if sd.enableRegister {
		_, err := json.Marshal(instance.GetMetadata())
		if err != nil {
			return perrors.WithMessagef(err, "service (%s), id (%s), host (%s)", instance.GetServiceName(), instance.GetId(), instance.GetHost())
		}
		//s := string(m)
		logger.Infof("Write Current Service Instance Metadata to Kubernetes pod. Current pod name: ",
			sd.currentHostname)
	}

	return nil
}

func (sd *kubernetesServiceDiscovery) Update(instance registry.ServiceInstance) error {
	return sd.Register(instance)
}

func (sd *kubernetesServiceDiscovery) Unregister(instance registry.ServiceInstance) error {
	sd.localServiceInstance = nil

	if sd.enableRegister {
		logger.Infof("Remove Current Service Instance from Kubernetes pod. Current pod name: ",
			sd.currentHostname)
	}

	return nil
}

// GetDefaultPageSize will return the constant registry.DefaultPageSize
func (sd *kubernetesServiceDiscovery) GetDefaultPageSize() int {
	return registry.DefaultPageSize
}

func (sd *kubernetesServiceDiscovery) GetServices() *gxset.HashSet {
	panic("implement me")
}

func (sd *kubernetesServiceDiscovery) GetInstances(serviceName string) []registry.ServiceInstance {
	panic("implement me")
}

func (sd *kubernetesServiceDiscovery) GetInstancesByPage(serviceName string, offset int, pageSize int) gxpage.Pager {
	panic("implement me")
}

func (sd *kubernetesServiceDiscovery) GetHealthyInstancesByPage(serviceName string, offset int, pageSize int, healthy bool) gxpage.Pager {
	panic("implement me")
}

func (sd *kubernetesServiceDiscovery) GetRequestInstances(serviceNames []string, offset int, requestedSize int) map[string]gxpage.Pager {
	panic("implement me")
}

func (sd *kubernetesServiceDiscovery) AddListener(listener *registry.ServiceInstancesChangedListener) error {
	panic("implement me")
}

func (sd *kubernetesServiceDiscovery) DispatchEventByServiceName(serviceName string) error {
	panic("implement me")
}

func (sd *kubernetesServiceDiscovery) DispatchEventForInstances(serviceName string, instances []registry.ServiceInstance) error {
	panic("implement me")
}

func (sd *kubernetesServiceDiscovery) DispatchEvent(event *registry.ServiceInstancesChangedEvent) error {
	panic("implement me")
}
