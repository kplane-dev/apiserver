package webhook

import (
	"fmt"
	"net"
	"net/url"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/labels"
	corelisters "k8s.io/client-go/listers/core/v1"
	discoverylisters "k8s.io/client-go/listers/discovery/v1"
)

type directServiceResolver struct {
	services              corelisters.ServiceLister
	endpointSlices        discoverylisters.EndpointSliceLister
	enableEndpointRouting bool
	hostname              string
}

func newDirectServiceResolver(services corelisters.ServiceLister, endpointSlices discoverylisters.EndpointSliceLister, enableEndpointRouting bool, hostname string) *directServiceResolver {
	return &directServiceResolver{
		services:              services,
		endpointSlices:        endpointSlices,
		enableEndpointRouting: enableEndpointRouting,
		hostname:              hostname,
	}
}

func (r *directServiceResolver) ResolveEndpoint(namespace, name string, port int32) (*url.URL, error) {
	if len(name) == 0 || len(namespace) == 0 || port == 0 {
		return nil, fmt.Errorf("cannot resolve an empty service name or namespace or port")
	}

	// resolve kubernetes.default.svc locally
	if name == "kubernetes" && namespace == corev1.NamespaceDefault && port == 443 {
		if u, err := url.Parse(r.hostname); err == nil {
			return u, nil
		}
	}

	if r.services == nil {
		return nil, fmt.Errorf("service lister is not configured")
	}
	svc, err := r.services.Services(namespace).Get(name)
	if err != nil {
		return nil, err
	}

	if !r.enableEndpointRouting {
		ip := svc.Spec.ClusterIP
		if ip == "" || ip == "None" {
			return nil, fmt.Errorf("service %q has no ClusterIP", name)
		}
		return &url.URL{Scheme: "https", Host: net.JoinHostPort(ip, strconv.Itoa(int(port)))}, nil
	}

	if r.endpointSlices == nil {
		return nil, fmt.Errorf("endpointslice lister is not configured")
	}
	targetName, targetPort := serviceTargetPort(svc, port)

	slices, err := r.endpointSlices.EndpointSlices(namespace).List(labels.SelectorFromSet(labels.Set{discoveryv1.LabelServiceName: name}))
	if err != nil {
		return nil, err
	}
	if len(slices) == 0 {
		return nil, fmt.Errorf("no endpointslices found for service %q", name)
	}

	epPort := port
	if targetPort != 0 {
		epPort = targetPort
	} else if targetName != "" {
		found := false
		for i := range slices {
			for _, p := range slices[i].Ports {
				if p.Name != nil && *p.Name == targetName && p.Port != nil {
					epPort = *p.Port
					found = true
					break
				}
			}
			if found {
				break
			}
		}
	}

	addr, err := firstEndpointAddress(slices)
	if err != nil {
		return nil, err
	}

	return &url.URL{Scheme: "https", Host: net.JoinHostPort(addr, strconv.Itoa(int(epPort)))}, nil
}

func serviceTargetPort(svc *corev1.Service, servicePort int32) (targetName string, targetPort int32) {
	for _, sp := range svc.Spec.Ports {
		if sp.Port != servicePort {
			continue
		}
		if sp.TargetPort.Type == 1 /* String */ {
			return sp.TargetPort.StrVal, 0
		}
		if sp.TargetPort.IntVal != 0 {
			return "", int32(sp.TargetPort.IntVal)
		}
		return "", servicePort
	}
	return "", servicePort
}

func firstEndpointAddress(slices []*discoveryv1.EndpointSlice) (string, error) {
	for i := range slices {
		for _, ep := range slices[i].Endpoints {
			ready := true
			if ep.Conditions.Ready != nil {
				ready = *ep.Conditions.Ready
			}
			if !ready {
				continue
			}
			if len(ep.Addresses) == 0 {
				continue
			}
			return ep.Addresses[0], nil
		}
	}
	return "", fmt.Errorf("no ready endpoint addresses found")
}
