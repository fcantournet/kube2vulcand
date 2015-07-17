# kube2vulcand
## Goals & design
This was written as part of a POC for a Kubernetes over CoreOS cluster and is NOT READY for prod. You have been warned.
(Part of) the code and the general idea is shamelessly stolen from kube2sky

The goal is to configure vulcand (a distributed http reverse-proxy with configuration stored in etcd) to expose the services in kubernetes which were declared as `type: loadBalancer`
In kubernetes `LoadBalancer` implies `NodePort` which means the service will be available (via kube-proxy) at nodeIP:NodePort (in a configurable range)

kube2vulcand watchs the kube-apiserver, and whenever a service definition changes (new, delete, or modification), it updates the vulcand accordingly.

## Example 
[TODO]

