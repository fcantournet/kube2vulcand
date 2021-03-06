/*
Copyright 2014 The Kubernetes Authors All rights reserved.

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

// kube2vulcand is a bridge between Kubernetes and SkyDNS.  It watches the
// Kubernetes master for changes in Services and manifests them into etcd for
// SkyDNS to serve as DNS records.
package main

import (
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"net/url"
	"os"
	"time"

	kapi "github.com/GoogleCloudPlatform/kubernetes/pkg/api"
	kclient "github.com/GoogleCloudPlatform/kubernetes/pkg/client"
	kcache "github.com/GoogleCloudPlatform/kubernetes/pkg/client/cache"
	kclientcmd "github.com/GoogleCloudPlatform/kubernetes/pkg/client/clientcmd"
	kframework "github.com/GoogleCloudPlatform/kubernetes/pkg/controller/framework"
	kSelector "github.com/GoogleCloudPlatform/kubernetes/pkg/fields"
	kLabel "github.com/GoogleCloudPlatform/kubernetes/pkg/labels"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/util"
	"github.com/golang/glog"

	vulcandapi "github.com/mailgun/vulcand/api"
	vulcandng "github.com/mailgun/vulcand/engine"
)

var (
	argKubecfgFile   = flag.String("kubecfg_file", "", "Location of kubecfg file for access to kubernetes service")
	argKubeMasterURL = flag.String("kube_master_url", "http://127.0.0.1:8080", "URL to reach kubernetes master. Env variables in this flag will be expanded.")
	argVulcandURL    = flag.String("vulcand_url", "http://127.0.0.1:8182", "Url to one of the vulcand server")
)

const (
	// Resync period for the kube controller loop.
	resyncPeriod = 30 * time.Minute
)

type nameNamespace struct {
	name      string
	namespace string
}

type kube2vulcand struct {
	//VulcandClient
	vulcandClient *vulcandapi.Client
	// A cache that contains all the endpoints in the system.
	endpointsStore kcache.Store
	// A cache that contains all the servicess in the system.
	servicesStore kcache.Store
	// Lock for controlling access to headless services.
	//mlock sync.Mutex
}

// Returns a cache.ListWatch that gets all changes to services.
func createServiceLW(kubeClient *kclient.Client) *kcache.ListWatch {
	return kcache.NewListWatchFromClient(kubeClient, "services", kapi.NamespaceAll, kSelector.Everything())
}

func (kv *kube2vulcand) addVulcandBackend(i instance) error {
	backend, err := vulcandng.NewHTTPBackend(i.Name, vulcandng.HTTPBackendSettings{})
	if err != nil {
		return err
	}
	return kv.vulcandClient.UpsertBackend(*backend)
}

func (kv *kube2vulcand) addVulcandFrontend(i instance) error {
	route := fmt.Sprintf("Host(`%s.<whatever>`)", i.Name)
	frontend, err := vulcandng.NewHTTPFrontend(i.Name, i.Name, route, vulcandng.HTTPFrontendSettings{})
	if err != nil {
		glog.Errorf("addVulcandFrontend: Failed to create HTTPFrontend : ", err)
		return err
	}
	return kv.vulcandClient.UpsertFrontend(*frontend, 0)
}

type vulcandServer struct {
	URL  string
	Name string
}

type instance struct {
	Name    string // admin.os-identity or os-identity
	Servers []vulcandServer
}

func (kv *kube2vulcand) newService(obj interface{}) {
	if s, ok := obj.(*kapi.Service); ok {
		// We only care about external services, which are declared as LoadBalancer (because they cloudprovider to make one)
		if s.Spec.Type == kapi.ServiceTypeLoadBalancer {
			// TODO: refactor to use the same KubeClient as the service poller ?
			kubeClient, err := newKubeClient()
			if err != nil {
				glog.Errorf("Failed to create a kubernetes client: %v", err)
				return
			}
			nodesList, err := kubeClient.Nodes().List(kLabel.Everything(), kSelector.Everything())
			if err != nil {
				glog.Errorf("Failed get nodes list: %v", err)
				return
			}
			nodes := nodesList.Items
			instances := []instance{}
			for _, port := range s.Spec.Ports {
				if port.Protocol != kapi.ProtocolTCP {
					continue
				}
				var instanceName string
				if port.Name == "" {
					instanceName = s.Name
				} else {
					instanceName = port.Name + "." + s.Name
				}
				currentInstance := instance{
					Name: instanceName,
				}
				for _, node := range nodes {
					// POPO
					url := fmt.Sprintf("http://%s:%d", node.Status.Addresses[0].Address, port.NodePort)
					s := vulcandServer{
						URL:  url,
						Name: getHash(url),
					}
					currentInstance.Servers = append(currentInstance.Servers, s)
				}
				instances = append(instances, currentInstance)
			}
			for _, i := range instances {
				if err := kv.addVulcandBackend(i); err != nil {
					glog.Errorf("Failed to add Vulcand Backend : ", err)
				}
				if err := kv.addVulcandFrontend(i); err != nil {
					glog.Errorf("Failed to add Vulcand Frontend : ", err)
				}
				for _, server := range i.Servers {
					srv, err := vulcandng.NewServer(server.Name, server.URL)
					if err != nil {
						glog.Errorf("Failed to create a Vulcand server", err)
						continue
					}
					if err := kv.vulcandClient.UpsertServer(vulcandng.BackendKey{Id: i.Name}, *srv, 0); err != nil {
						glog.Errorf("Failed to create Vulcand Server : ", err)
					}
				}
			}
		}
	}
}

func (kv *kube2vulcand) removeService(obj interface{}) {
	if s, ok := obj.(*kapi.Service); ok {
		if s.Spec.Type == kapi.ServiceTypeLoadBalancer {
			for _, port := range s.Spec.Ports {
				if port.Protocol != kapi.ProtocolTCP {
					continue
				}
				var instanceName string
				if port.Name == "" {
					instanceName = s.Name
				} else {
					instanceName = port.Name + "." + s.Name
				}
				kv.vulcandClient.DeleteFrontend(vulcandng.FrontendKey{Id: instanceName})
				kv.vulcandClient.DeleteBackend(vulcandng.BackendKey{Id: instanceName})
			}
		}
	}
}

func (kv *kube2vulcand) updateService(oldObj, newObj interface{}) {
	// TODO: Avoid unwanted updates.
	kv.removeService(oldObj)
	kv.newService(newObj)
}

func newVulcandClient(vulcandURL string) (*vulcandapi.Client, error) {
	client := vulcandapi.NewClient(vulcandURL, nil)
	if client != nil {
		return client, nil
	}
	return nil, errors.New("Couldn't create VulcandClient !")
}

func getKubeMasterURL() (string, error) {
	parsedURL, err := url.Parse(os.ExpandEnv(*argKubeMasterURL))
	if err != nil {
		return "", fmt.Errorf("failed to parse --kube_master_url %s - %v", *argKubeMasterURL, err)
	}
	if parsedURL.Scheme == "" || parsedURL.Host == "" || parsedURL.Host == ":" {
		return "", fmt.Errorf("invalid --kube_master_url specified %s", *argKubeMasterURL)
	}
	return parsedURL.String(), nil
}

// TODO: evaluate using pkg/client/clientcmd
func newKubeClient() (*kclient.Client, error) {
	var (
		config    *kclient.Config
		err       error
		masterURL string
	)
	if *argKubeMasterURL != "" {
		masterURL, err = getKubeMasterURL()
		if err != nil {
			return nil, err
		}
	}
	if *argKubecfgFile == "" {
		if masterURL == "" {
			return nil, fmt.Errorf("--kube_master_url must be set when --kubecfg_file is not set")
		}
		config = &kclient.Config{
			Host:    masterURL,
			Version: "v1",
		}
	} else {
		overrides := &kclientcmd.ConfigOverrides{}
		if masterURL != "" {
			overrides.ClusterInfo.Server = masterURL
		}
		if config, err = kclientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			&kclientcmd.ClientConfigLoadingRules{ExplicitPath: *argKubecfgFile},
			overrides).ClientConfig(); err != nil {
			return nil, err
		}
	}
	glog.Infof("Using %s for kubernetes master", config.Host)
	glog.Infof("Using kubernetes API %s", config.Version)
	return kclient.New(config)
}

func watchForServices(kubeClient *kclient.Client, kv *kube2vulcand) kcache.Store {
	serviceStore, serviceController := kframework.NewInformer(
		createServiceLW(kubeClient),
		&kapi.Service{},
		resyncPeriod,
		kframework.ResourceEventHandlerFuncs{
			AddFunc:    kv.newService,
			DeleteFunc: kv.removeService,
			UpdateFunc: kv.updateService,
		},
	)
	go serviceController.Run(util.NeverStop)
	return serviceStore
}

func getHash(text string) string {
	h := fnv.New32a()
	h.Write([]byte(text))
	return fmt.Sprintf("%x", h.Sum32())
}

func main() {
	flag.Parse()
	var err error
	kv := kube2vulcand{}

	if kv.vulcandClient, err = newVulcandClient(*argVulcandURL); err != nil {
		glog.Fatalf("Failed to create vulcand client - %v", err)
	}

	kubeClient, err := newKubeClient()
	if err != nil {
		glog.Fatalf("Failed to create a kubernetes client: %v", err)
	}

	kv.servicesStore = watchForServices(kubeClient, &kv)

	select {}
}
