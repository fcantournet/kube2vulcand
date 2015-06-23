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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	kapi "github.com/GoogleCloudPlatform/kubernetes/pkg/api"
	kclient "github.com/GoogleCloudPlatform/kubernetes/pkg/client"
	kcache "github.com/GoogleCloudPlatform/kubernetes/pkg/client/cache"
	kclientcmd "github.com/GoogleCloudPlatform/kubernetes/pkg/client/clientcmd"
	kframework "github.com/GoogleCloudPlatform/kubernetes/pkg/controller/framework"
	kSelector "github.com/GoogleCloudPlatform/kubernetes/pkg/fields"
	kLabel "github.com/GoogleCloudPlatform/kubernetes/pkg/labels"
	tools "github.com/GoogleCloudPlatform/kubernetes/pkg/tools"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/util"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/util/wait"
	etcd "github.com/coreos/go-etcd/etcd"
	"github.com/golang/glog"
	skymsg "github.com/skynetservices/skydns/msg"

	vulcandapi "github.com/mailgun/vulcand/api"
	vulcandng "github.com/mailgun/vulcand/engine"
)

var (
	argDomain              = flag.String("domain", "cluster.local", "domain under which to create names")
	argEtcdMutationTimeout = flag.Duration("etcd_mutation_timeout", 10*time.Second, "crash after retrying etcd mutation for a specified duration")
	argEtcdServer          = flag.String("etcd-server", "http://127.0.0.1:2379", "URL to etcd server")
	argKubecfgFile         = flag.String("kubecfg_file", "", "Location of kubecfg file for access to kubernetes service")
	argKubeMasterURL       = flag.String("kube_master_url", "http://127.0.0.1:8080", "URL to reach kubernetes master. Env variables in this flag will be expanded.")
	argVulcandURL          = flag.String("vulcand_url", "http://127.0.0.1:8182", "Url to one of the vulcand server")
)

const (
	// Maximum number of attempts to connect to etcd server.
	maxConnectAttempts = 12
	// Resync period for the kube controller loop.
	resyncPeriod = 30 * time.Minute
	// A subdomain added to the user specified domain for all services.
	serviceSubdomain = "svc"
)

type etcdClient interface {
	Set(path, value string, ttl uint64) (*etcd.Response, error)
	RawGet(key string, sort, recursive bool) (*etcd.RawResponse, error)
	Delete(path string, recursive bool) (*etcd.Response, error)
}

type nameNamespace struct {
	name      string
	namespace string
}

type kube2vulcand struct {
	//VulcandClient
	vulcandClient *vulcandapi.Client
	// Etcd client.
	etcdClient etcdClient
	// DNS domain name.
	domain string
	// Etcd mutation timeout.
	etcdMutationTimeout time.Duration
	// A cache that contains all the endpoints in the system.
	endpointsStore kcache.Store
	// A cache that contains all the servicess in the system.
	servicesStore kcache.Store
	// Lock for controlling access to headless services.
	mlock sync.Mutex
}

// Removes 'subdomain' from etcd.
func (kv *kube2vulcand) removeDNS(subdomain string) error {
	glog.V(2).Infof("Removing %s from DNS", subdomain)
	resp, err := kv.etcdClient.RawGet(skymsg.Path(subdomain), false, true)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusNotFound {
		glog.V(2).Infof("Subdomain %q does not exist in etcd", subdomain)
		return nil
	}
	_, err = kv.etcdClient.Delete(skymsg.Path(subdomain), true)
	return err
}

func (kv *kube2vulcand) writeSkyRecord(subdomain string, data string) error {
	// Set with no TTL, and hope that kubernetes events are accurate.
	_, err := kv.etcdClient.Set(skymsg.Path(subdomain), data, uint64(0))
	return err
}

// Generates skydns records for a headless service.
func (kv *kube2vulcand) newHeadlessService(subdomain string, service *kapi.Service, isNewStyleFormat bool) error {
	// Create an A record for every pod in the service.
	// This record must be periodically updated.
	// Format is as follows:
	// For a service x, with pods a and b create DNS records,
	// a.x.ns.domain. and, b.x.ns.domain.
	// TODO: Handle multi-port services.
	kv.mlock.Lock()
	defer kv.mlock.Unlock()
	key, err := kcache.MetaNamespaceKeyFunc(service)
	if err != nil {
		return err
	}
	e, exists, err := kv.endpointsStore.GetByKey(key)
	if err != nil {
		return fmt.Errorf("failed to get endpoints object from endpoints store - %v", err)
	}
	if !exists {
		glog.V(1).Infof("could not find endpoints for service %q in namespace %q. DNS records will be created once endpoints show up.", service.Name, service.Namespace)
		return nil
	}
	if e, ok := e.(*kapi.Endpoints); ok {
		return kv.generateRecordsForHeadlessService(subdomain, e, service, isNewStyleFormat)
	}
	return nil
}

func getSkyMsg(ip string, port int) *skymsg.Service {
	return &skymsg.Service{
		Host:     ip,
		Port:     port,
		Priority: 10,
		Weight:   10,
		Ttl:      30,
	}
}

func (kv *kube2vulcand) generateRecordsForHeadlessService(subdomain string, e *kapi.Endpoints, svc *kapi.Service, isNewStyleFormat bool) error {
	for idx := range e.Subsets {
		for subIdx := range e.Subsets[idx].Addresses {
			b, err := json.Marshal(getSkyMsg(e.Subsets[idx].Addresses[subIdx].IP, 0))
			if err != nil {
				return err
			}
			recordValue := string(b)
			recordLabel := getHash(recordValue)
			recordKey := buildDNSNameString(subdomain, recordLabel)

			glog.V(2).Infof("Setting DNS record: %v -> %q\n", recordKey, recordValue)
			if err := kv.writeSkyRecord(recordKey, recordValue); err != nil {
				return err
			}
			if isNewStyleFormat {
				for portIdx := range e.Subsets[idx].Ports {
					endpointPort := &e.Subsets[idx].Ports[portIdx]
					portSegment := buildPortSegmentString(endpointPort.Name, endpointPort.Protocol)
					if portSegment != "" {
						err := kv.generateSRVRecord(subdomain, portSegment, recordLabel, recordKey, endpointPort.Port)
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}

	return nil
}

func (kv *kube2vulcand) getServiceFromEndpoints(e *kapi.Endpoints) (*kapi.Service, error) {
	key, err := kcache.MetaNamespaceKeyFunc(e)
	if err != nil {
		return nil, err
	}
	obj, exists, err := kv.servicesStore.GetByKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get service object from services store - %v", err)
	}
	if !exists {
		glog.V(1).Infof("could not find service for endpoint %q in namespace %q", e.Name, e.Namespace)
		return nil, nil
	}
	if svc, ok := obj.(*kapi.Service); ok {
		return svc, nil
	}
	return nil, fmt.Errorf("got a non service object in services store %v", obj)
}

func buildDNSNameString(labels ...string) string {
	var res string
	for _, label := range labels {
		if res == "" {
			res = label
		} else {
			res = fmt.Sprintf("%s.%s", label, res)
		}
	}
	return res
}

func (kv *kube2vulcand) addDNSUsingEndpoints(subdomain string, e *kapi.Endpoints, isNewStyleFormat bool) error {
	kv.mlock.Lock()
	defer kv.mlock.Unlock()
	svc, err := kv.getServiceFromEndpoints(e)
	if err != nil {
		return err
	}
	if svc == nil || kapi.IsServiceIPSet(svc) {
		// No headless service found corresponding to endpoints object.
		return nil
	}
	// Remove existing DNS entry.
	if err := kv.removeDNS(subdomain); err != nil {
		return err
	}
	return kv.generateRecordsForHeadlessService(subdomain, e, svc, isNewStyleFormat)
}

// func (kv *kube2vulcand) handleEndpointAdd(obj interface{}) {
// 	if e, ok := obj.(*kapi.Endpoints); ok {
// 		svc, err := kv.getServiceFromEndpoints(e)
// 		if err != nil {
// 			return
// 		}
// 		for subset := range e.Subsets {
// 			url := "http://" + subset.addresses[0].Ip
// 			srv, err := vulcandng.NewServer(e.Subsets[0]., u string)
// 			kv.vulcandClient.UpsertServer(vulcandng.BackendKey{id: svc.Name}, srv engine.Server, ttl time.Duration)
//
// 		}
// 		pa
// 		//name := buildDNSNameString(kv.domain, e.Namespace, e.Name)
// 		//kv.mutateEtcdOrDie(func() error { return kv.addDNSUsingEndpoints(name, e, false) })
// 		//name = buildDNSNameString(kv.domain, serviceSubdomain, e.Namespace, e.Name)
// 		//kv.mutateEtcdOrDie(func() error { return kv.addDNSUsingEndpoints(name, e, true) })
// 	}
// }

func (kv *kube2vulcand) generateRecordsForPortalService(subdomain string, service *kapi.Service, isNewStyleFormat bool) error {
	b, err := json.Marshal(getSkyMsg(service.Spec.ClusterIP, 0))
	if err != nil {
		return err
	}
	recordValue := string(b)
	recordKey := subdomain
	recordLabel := ""
	if isNewStyleFormat {
		recordLabel = getHash(recordValue)
		if err != nil {
			return err
		}
		recordKey = buildDNSNameString(subdomain, recordLabel)
	}

	glog.V(2).Infof("Setting DNS record: %v -> %q, with recordKey: %v\n", subdomain, recordValue, recordKey)
	if err := kv.writeSkyRecord(recordKey, recordValue); err != nil {
		return err
	}
	if !isNewStyleFormat {
		return nil
	}
	// Generate SRV Records
	for i := range service.Spec.Ports {
		port := &service.Spec.Ports[i]
		portSegment := buildPortSegmentString(port.Name, port.Protocol)
		if portSegment != "" {
			err = kv.generateSRVRecord(subdomain, portSegment, recordLabel, subdomain, port.Port)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func buildPortSegmentString(portName string, portProtocol kapi.Protocol) string {
	if portName == "" {
		// we don't create a random name
		return ""
	}

	if portProtocol == "" {
		glog.Errorf("Port Protocol not set. port segment string cannot be created.")
		return ""
	}

	return fmt.Sprintf("_%s._%s", portName, strings.ToLower(string(portProtocol)))
}

func (kv *kube2vulcand) generateSRVRecord(subdomain, portSegment, recordName, cName string, portNumber int) error {
	recordKey := buildDNSNameString(subdomain, portSegment, recordName)
	srvRec, err := json.Marshal(getSkyMsg(cName, portNumber))
	if err != nil {
		return err
	}
	if err := kv.writeSkyRecord(recordKey, string(srvRec)); err != nil {
		return err
	}
	return nil
}

func (kv *kube2vulcand) addDNS(subdomain string, service *kapi.Service, isNewStyleFormat bool) error {
	if len(service.Spec.Ports) == 0 {
		glog.Fatalf("unexpected service with no ports: %v", service)
	}
	// if ClusterIP is not set, a DNS entry should not be created
	if !kapi.IsServiceIPSet(service) {
		return kv.newHeadlessService(subdomain, service, isNewStyleFormat)
	}
	return kv.generateRecordsForPortalService(subdomain, service, isNewStyleFormat)
}

// Implements retry logic for arbitrary mutator. Crashes after retrying for
// etcd_mutation_timeout.
func (kv *kube2vulcand) mutateEtcdOrDie(mutator func() error) {
	timeout := time.After(kv.etcdMutationTimeout)
	for {
		select {
		case <-timeout:
			glog.Fatalf("Failed to mutate etcd for %v using mutator: %v", kv.etcdMutationTimeout, mutator)
		default:
			if err := mutator(); err != nil {
				delay := 50 * time.Millisecond
				glog.V(1).Infof("Failed to mutate etcd using mutator: %v due to: %v. Will retry in: %v", mutator, err, delay)
				time.Sleep(delay)
			} else {
				return
			}
		}
	}
}

// Returns a cache.ListWatch that gets all changes to services.
func createServiceLW(kubeClient *kclient.Client) *kcache.ListWatch {
	return kcache.NewListWatchFromClient(kubeClient, "services", kapi.NamespaceAll, kSelector.Everything())
}

// Returns a cache.ListWatch that gets all changes to endpoints.
func createEndpointsLW(kubeClient *kclient.Client) *kcache.ListWatch {
	return kcache.NewListWatchFromClient(kubeClient, "endpoints", kapi.NamespaceAll, kSelector.Everything())
}

const vulcanetcdpath string = "/vulcand/"

func (kv *kube2vulcand) addVulcandBackend(s *kapi.Service) error {
	//path := vulcanetcdpath + "backends/" + s.Name + "/backend"
	//value := []byte(`{"type": "http"}`)
	//_, err := kv.etcdClient.Set(path, value, 0)
	//return err
	backend, err := vulcandng.NewHTTPBackend(s.Name, vulcandng.HTTPBackendSettings{})
	if err != nil {
		return err
	}
	return kv.vulcandClient.UpsertBackend(*backend)
}

func (kv *kube2vulcand) addVulcandFrontend(s *kapi.Service) error {
	//path := vulcanetcdpath + "frontends/" + s.Name + "/frontend"
	//value := []byte(`{"type": "http", "BackendId": }`)
	route := fmt.Sprintf("Host(`%s.<whatever>`) && Path(`/`)", s.Name)
	frontend, err := vulcandng.NewHTTPFrontend(s.Name, s.Name, route, vulcandng.HTTPFrontendSettings{})
	if err != nil {
		return err
	}
	return kv.vulcandClient.UpsertFrontend(*frontend, 0)
}

type vulcandServer struct {
	URL  string
	Name string
}

func (kv *kube2vulcand) newService(obj interface{}) {
	if s, ok := obj.(*kapi.Service); ok {
		//TODO(artfulcoder) stop adding and deleting old-format string for service
		if s.Spec.Type == kapi.ServiceTypeLoadBalancer {
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
			servers := []vulcandServer{}
			for _, port := range s.Spec.Ports {
				if port.Protocol != kapi.ProtocolTCP {
					continue
				}
				for _, node := range nodes {
					// POPO
					url := fmt.Sprintf("http://%s:%d", node.Status.Addresses[0].Address, port.NodePort)
					s := vulcandServer{
						URL:  url,
						Name: getHash(url),
					}
					servers = append(servers, s)
				}
			}
			if len(servers) > 0 {
				kv.addVulcandFrontend(s)
				kv.addVulcandBackend(s)
				for _, server := range servers {
					srv, err := vulcandng.NewServer(server.Name, server.URL)
					if err != nil {
						glog.Errorf("Failed to create a kubernetes client: %v", err)
						continue
					}
					kv.vulcandClient.UpsertServer(vulcandng.BackendKey{Id: s.Name}, *srv, 0)
				}
			}
			//name := buildDNSNameString(kv.domain, s.Namespace, s.Name)
			//kv.mutateEtcdOrDie(func() error { return kv.addDNS(name, s, false) })
			//name = buildDNSNameString(kv.domain, serviceSubdomain, s.Namespace, s.Name)
			//kv.mutateEtcdOrDie(func() error { return kv.addDNS(name, s, true) })
		}
	}
}

func (kv *kube2vulcand) removeService(obj interface{}) {
	if s, ok := obj.(*kapi.Service); ok {
		if s.Spec.Type == kapi.ServiceTypeLoadBalancer {
			kv.vulcandClient.DeleteFrontend(vulcandng.FrontendKey{Id: s.Name})
			kv.vulcandClient.DeleteBackend(vulcandng.BackendKey{Id: s.Name})
		}
		//		name := buildDNSNameString(kv.domain, s.Namespace, s.Name)
		//		kv.mutateEtcdOrDie(func() error { return kv.removeDNS(name) })
		//		name = buildDNSNameString(kv.domain, serviceSubdomain, s.Namespace, s.Name)
		//		kv.mutateEtcdOrDie(func() error { return kv.removeDNS(name) })
	}
}

func (kv *kube2vulcand) updateService(oldObj, newObj interface{}) {
	// TODO: Avoid unwanted updates.
	kv.removeService(oldObj)
	kv.newService(newObj)
}

func newVulcandClient(vulcandserver string) (*vulcandapi.Client, error) {
	client := vulcandapi.NewClient(vulcandserver, nil)
	if client != nil {
		return client, nil
	}
	return nil, errors.New("Couldn't create VulcandClient !")
}

func newEtcdClient(etcdServer string) (*etcd.Client, error) {
	var (
		client *etcd.Client
		err    error
	)
	for attempt := 1; attempt <= maxConnectAttempts; attempt++ {
		if _, err = tools.GetEtcdVersion(etcdServer); err == nil {
			break
		}
		if attempt == maxConnectAttempts {
			break
		}
		glog.Infof("[Attempt: %d] Attempting access to etcd after 5 second sleep", attempt)
		time.Sleep(5 * time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd server: %v, error: %v", etcdServer, err)
	}
	glog.Infof("Etcd server found: %v", etcdServer)

	// loop until we have > 0 machines && machines[0] != ""
	poll, timeout := 1*time.Second, 10*time.Second
	if err := wait.Poll(poll, timeout, func() (bool, error) {
		if client = etcd.NewClient([]string{etcdServer}); client == nil {
			return false, fmt.Errorf("etcd.NewClient returned nil")
		}
		client.SyncCluster()
		machines := client.GetCluster()
		if len(machines) == 0 || len(machines[0]) == 0 {
			return false, nil
		}
		return true, nil
	}); err != nil {
		return nil, fmt.Errorf("Timed out after %s waiting for at least 1 synchronized etcd server in the cluster. Error: %v", timeout, err)
	}
	return client, nil
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

// func watchEndpoints(kubeClient *kclient.Client, kv *kube2vulcand) kcache.Store {
// 	eStore, eController := kframework.NewInformer(
// 		createEndpointsLW(kubeClient),
// 		&kapi.Endpoints{},
// 		resyncPeriod,
// 		kframework.ResourceEventHandlerFuncs{
// 			AddFunc: kv.handleEndpointAdd,
// 			UpdateFunc: func(oldObj, newObj interface{}) {
// 				// TODO: Avoid unwanted updates.
// 				kv.handleEndpointAdd(newObj)
// 			},
// 		},
// 	)
//
// 	go eController.Run(util.NeverStop)
// 	return eStore
// }

func getHash(text string) string {
	h := fnv.New32a()
	h.Write([]byte(text))
	return fmt.Sprintf("%x", h.Sum32())
}

func main() {
	flag.Parse()
	var err error
	// TODO: Validate input flags.
	domain := *argDomain
	if !strings.HasSuffix(domain, ".") {
		domain = fmt.Sprintf("%s.", domain)
	}
	kv := kube2vulcand{
		domain:              domain,
		etcdMutationTimeout: *argEtcdMutationTimeout,
	}

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
