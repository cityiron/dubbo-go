package kubernetes

import (
	"encoding/json"
	"fmt"
	"github.com/apache/dubbo-go/remoting/kubernetes/v2"
	"net/url"
	"os"
	"strconv"
	"sync"
)

import (
	gxset "github.com/dubbogo/gost/container/set"
	gxpage "github.com/dubbogo/gost/hash/page"
	perrors "github.com/pkg/errors"
	"go.uber.org/atomic"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
)

import (
	"github.com/apache/dubbo-go/common"
	"github.com/apache/dubbo-go/common/constant"
	"github.com/apache/dubbo-go/common/extension"
	"github.com/apache/dubbo-go/common/logger"
	"github.com/apache/dubbo-go/config"
	"github.com/apache/dubbo-go/registry"
)

const (
	// kubernetesPropertiesKey Stay in line with dubbo
	kubernetesPropertiesKey = "io.dubbo/metadata"
)

const (
	// EnvHostName host name
	EnvHostName = "HOSTNAME"
)

var (
	// 16 would be enough. We won't use concurrentMap because in most cases, there are not race condition
	instanceMap = make(map[string]registry.ServiceDiscovery, 16)
	initLock    sync.Mutex
)

// init will put the service discovery into extension
func init() {
	extension.SetServiceDiscovery(constant.KubernetesKey, newKubernetesServiceDiscovery)
}

// kubernetesServiceDiscovery is the implementation of service discovery based on client-go.
// There is a problem, the client-go for kubernetes does not support the id field.
// we will use the metadata to store the id of ServiceInstance
type kubernetesServiceDiscovery struct {
	currentHostname      string
	localServiceInstance registry.ServiceInstance
	namespace            string
	enableRegister       bool
	client               v2.Client
	url                  *common.URL

	cltLock sync.RWMutex

	// key: serviceName value: count
	serviceUpdateTime map[string]atomic.Uint32
}

// newKubernetesServiceDiscovery will create new service discovery instance
// use double-check pattern to reduce race condition
func newKubernetesServiceDiscovery(name string) (registry.ServiceDiscovery, error) {
	// double check, found instance from the cache of instanceMap.
	instance, ok := instanceMap[name]
	if ok {
		return instance, nil
	}

	initLock.Lock()
	defer initLock.Unlock()

	instance, ok = instanceMap[name]
	if ok {
		return instance, nil
	}

	// config valid.
	sdc, ok := config.GetBaseConfig().GetServiceDiscoveries(name)
	if !ok || len(sdc.RemoteRef) == 0 {
		return nil, perrors.New("could not init the instance because the config is invalid")
	}
	remoteConfig, ok := config.GetBaseConfig().GetRemoteConfig(sdc.RemoteRef)
	if !ok {
		return nil, perrors.New("could not find the remote config for name: " + sdc.RemoteRef)
	}

	url := common.NewURLWithOptions(
		common.WithParams(make(url.Values)),
		common.WithPassword(remoteConfig.Password),
		common.WithUsername(remoteConfig.Username),
		common.WithParamsValue(constant.REGISTRY_TIMEOUT_KEY, remoteConfig.TimeoutStr))
	url.Location = remoteConfig.Address

	chn := os.Getenv(EnvHostName)

	// enable registry
	er := remoteConfig.GetParam(constant.EnableRegister, "true")
	re, err := strconv.ParseBool(er)
	if err != nil {
		re = true
	}

	ns := remoteConfig.GetParam(constant.NameSpace, "default")

	k8sSd := &kubernetesServiceDiscovery{
		url:             url,
		enableRegister:  re,
		namespace:       ns,
		currentHostname: chn,
	}

	cl := &v2.Client{}
	if err := v2.ValidateClient(cl); err != nil {
		return nil, perrors.WithStack(err)
	}

	return k8sSd, nil
}

// nolint
func (sd *kubernetesServiceDiscovery) String() string {
	return fmt.Sprintf("kubernetes-service-discovery[%s]", sd.url)
}

// Close client be closed
func (sd *kubernetesServiceDiscovery) Destroy() error {
	sd.client.Close()
	return nil
}

// Register will register service in kubernetes
func (sd *kubernetesServiceDiscovery) Register(instance registry.ServiceInstance) error {
	sd.localServiceInstance = instance

	if sd.enableRegister {
		annotation := make(map[string]map[string]string, 4)
		annotation[kubernetesPropertiesKey] = instance.GetMetadata()

		m, err := json.Marshal(annotation)
		if err != nil {
			return perrors.WithMessagef(err, "service (%s), id (%s), host (%s)", instance.GetServiceName(), instance.GetId(), instance.GetHost())
		}

		_, err = sd.client.Kc.CoreV1().Pods(sd.namespace).Patch(sd.currentHostname, types.StrategicMergePatchType, m)
		if err != nil {
			return perrors.WithMessage(err, "patch in kubernetes pod ")
		}
		logger.Infof("[kubernetesServiceDiscovery] Write Current Service Instance Metadata to Kubernetes pod. Current pod name: ",
			sd.currentHostname)
	}

	return nil
}

// Register will update service in kubernetes
func (sd *kubernetesServiceDiscovery) Update(instance registry.ServiceInstance) error {
	return sd.Register(instance)
}

// Unregister will unregister the instance in kubernetes
func (sd *kubernetesServiceDiscovery) Unregister(instance registry.ServiceInstance) error {
	sd.localServiceInstance = nil

	if sd.enableRegister {
		//_, err = sd.client.kc.CoreV1().Pods(sd.namespace).Patch(sd.currentHostname, types.JSONPatchType, m)

		logger.Infof("[kubernetesServiceDiscovery] Remove Current Service Instance from Kubernetes pod. Current pod name: ",
			sd.currentHostname)
	}

	return nil
}

// GetDefaultPageSize will return the constant registry.DefaultPageSize
func (sd *kubernetesServiceDiscovery) GetDefaultPageSize() int {
	return registry.DefaultPageSize
}

// GetServices will return the all services in kubernetes
func (sd *kubernetesServiceDiscovery) GetServices() *gxset.HashSet {
	services, err := sd.client.Kc.CoreV1().Services(sd.namespace).List(metav1.ListOptions{})
	res := gxset.NewSet()
	if err != nil {
		logger.Errorf("[kubernetesServiceDiscovery] Could not query the services: %v", err)
		return res
	}

	for _, v := range services.Items {
		res.Add(v)
	}
	return res
}

// GetInstances will return the instances of serviceName and the group
func (sd *kubernetesServiceDiscovery) GetInstances(serviceName string) []registry.ServiceInstance {
	endpoints, err := sd.client.Kc.CoreV1().Endpoints(sd.namespace).Get(serviceName, metav1.GetOptions{})
	if err != nil {
		logger.Errorf("[kubernetesServiceDiscovery] Could not query the instances for service{%s}, error = err{%v} ",
			serviceName, err)
		return make([]registry.ServiceInstance, 0)
	}

	return sd.toServiceInstance(endpoints, serviceName)
}

// GetInstancesByPage will return the instances
func (sd *kubernetesServiceDiscovery) GetInstancesByPage(serviceName string, offset int, pageSize int) gxpage.Pager {
	all := sd.GetInstances(serviceName)
	res := make([]interface{}, 0, pageSize)
	// could not use res = all[a:b] here because the res should be []interface{}, not []ServiceInstance
	for i := offset; i < len(all) && i < offset+pageSize; i++ {
		res = append(res, all[i])
	}
	return gxpage.NewPage(offset, pageSize, res, len(all))
}

// GetHealthyInstancesByPage will return the instance
// In kubernetes, all service from endpoint instance's is healthy.
// However, the healthy parameter in this method maybe false. So we can not use that API.
// Thus, we must query all instances and then do filter
func (sd *kubernetesServiceDiscovery) GetHealthyInstancesByPage(serviceName string, offset int, pageSize int, healthy bool) gxpage.Pager {
	all := sd.GetInstances(serviceName)
	res := make([]interface{}, 0, pageSize)
	// could not use res = all[a:b] here because the res should be []interface{}, not []ServiceInstance
	var (
		i     = offset
		count = 0
	)
	for i < len(all) && count < pageSize {
		ins := all[i]
		if ins.IsHealthy() == healthy {
			res = append(res, all[i])
			count++
		}
		i++
	}
	return gxpage.NewPage(offset, pageSize, res, len(all))
}

// GetRequestInstances will return the instances
func (sd *kubernetesServiceDiscovery) GetRequestInstances(serviceNames []string, offset int, requestedSize int) map[string]gxpage.Pager {
	res := make(map[string]gxpage.Pager, len(serviceNames))
	for _, name := range serviceNames {
		res[name] = sd.GetInstancesByPage(name, offset, requestedSize)
	}
	return res
}

func (sd *kubernetesServiceDiscovery) AddListener(listener *registry.ServiceInstancesChangedListener) error {
	//listener.ServiceName
	return nil
}

func (sd *kubernetesServiceDiscovery) DispatchEventByServiceName(serviceName string) error {
	return sd.DispatchEventForInstances(serviceName, sd.GetInstances(serviceName))
}

func (sd *kubernetesServiceDiscovery) DispatchEventForInstances(serviceName string, instances []registry.ServiceInstance) error {
	return sd.DispatchEvent(registry.NewServiceInstancesChangedEvent(serviceName, instances))
}

func (sd *kubernetesServiceDiscovery) DispatchEvent(event *registry.ServiceInstancesChangedEvent) error {
	extension.GetGlobalDispatcher().Dispatch(event)
	return nil
}

func (sd *kubernetesServiceDiscovery) toServiceInstance(ep *v1.Endpoints, serviceName string) []registry.ServiceInstance {
	selector := sd.getServiceSelector(serviceName)

	re := make([]registry.ServiceInstance, 0)

	if len(selector) == 0 {
		return re
	}

	labelSelector := metav1.LabelSelector{MatchLabels: selector}

	options := metav1.ListOptions{
		LabelSelector: labels.Set(labelSelector.MatchLabels).String(),
		// TODO 100 is enough in most case.
		Limit: 100,
	}

	list, err := sd.client.Kc.CoreV1().Pods(sd.namespace).List(options)
	if err != nil {
		logger.Errorf("[kubernetesServiceDiscovery] Could not get the pods for service{%s}, error = err{%v} ",
			serviceName, err)
		return re
	}

	podMap := make(map[string]v1.Pod)
	for _, pod := range list.Items {
		podMap[pod.GetObjectMeta().GetName()] = pod
	}

	instancePorts := gxset.NewSet()
	// loop for port
	for i := range ep.Subsets {
		for i2 := range ep.Subsets[i].Ports {
			instancePorts.Add(ep.Subsets[i].Ports[i2].Port)
		}
	}
	// loop for address
	for i := range ep.Subsets {
		for i2 := range ep.Subsets[i].Addresses {
			ad := ep.Subsets[i].Addresses[i2]
			pod, ok := podMap[ad.TargetRef.Name]
			if !ok {
				logger.Warnf("[kubernetesServiceDiscovery] Unable to match Kubernetes Endpoint address with Pod. "+
					"EndpointAddress Hostname{%s}", ad.TargetRef.Name)
				continue
			}
			ip := ad.IP

			for i3 := range instancePorts.Values() {
				p := instancePorts.Values()[i3].(int32)
				md := pod.ObjectMeta.Annotations[kubernetesPropertiesKey]
				if len(md) <= 0 {
					logger.Warnf("[kubernetesServiceDiscovery] Unable to find Service Instance metadata in Pod Annotations. "+
						"Possibly cause: provider has not been initialized successfully. "+
						"EndpointAddress Hostname{%s}", ad.TargetRef.Name)
					continue
				}
				var mdm map[string]string
				json.Unmarshal([]byte(md), &mdm)

				dsi := &registry.DefaultServiceInstance{
					Id:          ip + ":" + strconv.FormatInt(int64(p), 10),
					ServiceName: serviceName,
					Host:        ip,
					Port:        int(p),
					Enable:      true,
					Healthy:     true,
					Metadata:    mdm,
				}
				re = append(re, dsi)
			}
		}
	}

	return re
}

func (sd *kubernetesServiceDiscovery) getServiceSelector(serviceName string) map[string]string {
	service, err := sd.client.Kc.CoreV1().Services(sd.namespace).Get(serviceName, metav1.GetOptions{})
	if err != nil {
		logger.Errorf("[kubernetesServiceDiscovery] Could not get the selector for service{%s}, error = err{%v} ",
			serviceName, err)
		return make(map[string]string, 0)
	}

	return service.Spec.Selector
}
