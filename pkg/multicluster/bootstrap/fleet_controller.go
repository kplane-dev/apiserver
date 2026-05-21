/*
Copyright 2026 The kplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package bootstrap

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	kplanev1 "github.com/kplane-dev/apiserver/pkg/apis/kplane/v1"
	mc "github.com/kplane-dev/apiserver/pkg/multicluster"
)

const (
	fleetWorkerCount  = 4
	fleetResyncPeriod = 30 * time.Second
	fleetCRDPollEvery = 250 * time.Millisecond
	fleetCRDPollTotal = 60 * time.Second
)

// FleetControllerOptions configures a FleetController.
type FleetControllerOptions struct {
	// RootCluster is the cluster ID of the root control plane where Fleet
	// objects live.
	RootCluster string

	// BaseLoopbackClientConfig is the apiserver's loopback client config.
	// The controller derives root-scoped clients from it.
	BaseLoopbackClientConfig *rest.Config

	// PathPrefix and ControlPlaneSegment configure cluster URL routing
	// (default: "/clusters/" and "control-plane").
	PathPrefix          string
	ControlPlaneSegment string

	// EnsureCluster is the per-member bootstrap hook. The controller invokes
	// it for every desired Fleet member, exactly like organic traffic would
	// trigger via mcOpts.OnClusterSelected.
	EnsureCluster func(clusterID string)

	// ClientForCluster returns a kube client scoped to a given VCP.
	// Used by status reconciliation to probe per-cluster readiness.
	ClientForCluster func(clusterID string) (kubernetes.Interface, error)

	// ResyncInterval controls how often each known Fleet is re-enqueued to
	// refresh its readiness. Defaults to 30s.
	ResyncInterval time.Duration
}

// FleetController watches Fleet objects in the root control plane and
// reconciles them by priming N virtual control planes via EnsureCluster.
type FleetController struct {
	opts FleetControllerOptions

	rootHost string

	apiext  apiextensionsclientset.Interface
	dyn     dynamic.Interface
	factory dynamicinformer.DynamicSharedInformerFactory

	informer cache.SharedIndexInformer
	queue    workqueue.TypedRateLimitingInterface[string]

	started atomic.Bool

	mu       sync.Mutex
	known    map[string]struct{}
	stopCh   <-chan struct{}
	resyncCh chan struct{}
}

// NewFleetController constructs a controller. It does not start any workers;
// call Start to install the Fleet CRD and begin reconciling.
func NewFleetController(opts FleetControllerOptions) (*FleetController, error) {
	if opts.BaseLoopbackClientConfig == nil {
		return nil, fmt.Errorf("FleetController: BaseLoopbackClientConfig is required")
	}
	if opts.RootCluster == "" {
		opts.RootCluster = mc.DefaultClusterName
	}
	if opts.ResyncInterval <= 0 {
		opts.ResyncInterval = fleetResyncPeriod
	}

	rootCfg := rest.CopyConfig(opts.BaseLoopbackClientConfig)
	host, err := mc.ClusterHost(rootCfg.Host, mc.Options{
		PathPrefix:          opts.PathPrefix,
		ControlPlaneSegment: opts.ControlPlaneSegment,
	}, opts.RootCluster)
	if err != nil {
		return nil, fmt.Errorf("FleetController: build root host: %w", err)
	}
	rootCfg.Host = host

	apiext, err := apiextensionsclientset.NewForConfig(rootCfg)
	if err != nil {
		return nil, fmt.Errorf("FleetController: apiextensions client: %w", err)
	}
	dyn, err := dynamic.NewForConfig(rootCfg)
	if err != nil {
		return nil, fmt.Errorf("FleetController: dynamic client: %w", err)
	}

	c := &FleetController{
		opts:     opts,
		rootHost: rootCfg.Host,
		apiext:   apiext,
		dyn:      dyn,
		known:    map[string]struct{}{},
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "mc_fleet_controller"},
		),
	}
	return c, nil
}

// Start installs the Fleet CRD into the root control plane (idempotently) and
// then begins reconciling Fleet objects. It returns immediately; work runs in
// background goroutines until stopCh is closed.
func (c *FleetController) Start(stopCh <-chan struct{}) {
	if c == nil {
		return
	}
	if !c.started.CompareAndSwap(false, true) {
		return
	}
	c.mu.Lock()
	c.stopCh = stopCh
	c.mu.Unlock()
	go c.runLifecycle(stopCh)
}

func (c *FleetController) runLifecycle(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()

	if err := c.installCRD(stopCh); err != nil {
		klog.Errorf("mc.fleet CRD install failed: %v", err)
		return
	}
	if err := c.waitForCRDEstablished(stopCh); err != nil {
		klog.Errorf("mc.fleet CRD never became established: %v", err)
		return
	}

	c.factory = dynamicinformer.NewDynamicSharedInformerFactory(c.dyn, c.opts.ResyncInterval)
	gvr := schema.GroupVersionResource{
		Group:    kplanev1.GroupName,
		Version:  kplanev1.Version,
		Resource: "fleets",
	}
	c.informer = c.factory.ForResource(gvr).Informer()
	_, _ = c.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.onFleetAdd,
		UpdateFunc: c.onFleetUpdate,
		DeleteFunc: c.onFleetDelete,
	})

	c.factory.Start(stopCh)
	if !cache.WaitForCacheSync(stopCh, c.informer.HasSynced) {
		klog.Warningf("mc.fleet informer cache sync aborted")
		return
	}

	for i := 0; i < fleetWorkerCount; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}
	go c.runPeriodicResync(stopCh)

	<-stopCh
	c.queue.ShutDown()
}

func (c *FleetController) installCRD(stopCh <-chan struct{}) error {
	desired := kplanev1.FleetCRD()
	ctx, cancel := contextFromStop(stopCh, 30*time.Second)
	defer cancel()

	existing, err := c.apiext.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, desired.Name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		_, createErr := c.apiext.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, desired, metav1.CreateOptions{})
		if createErr != nil && !apierrors.IsAlreadyExists(createErr) {
			return fmt.Errorf("create CRD %s: %w", desired.Name, createErr)
		}
		klog.Infof("mc.fleet installed CRD %s", desired.Name)
		return nil
	case err != nil:
		return fmt.Errorf("get CRD %s: %w", desired.Name, err)
	}

	desired.ResourceVersion = existing.ResourceVersion
	if _, err := c.apiext.ApiextensionsV1().CustomResourceDefinitions().Update(ctx, desired, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update CRD %s: %w", desired.Name, err)
	}
	klog.Infof("mc.fleet updated CRD %s", desired.Name)
	return nil
}

func (c *FleetController) waitForCRDEstablished(stopCh <-chan struct{}) error {
	deadline := time.Now().Add(fleetCRDPollTotal)
	for {
		select {
		case <-stopCh:
			return fmt.Errorf("stopped while waiting for CRD")
		default:
		}
		ctx, cancel := contextFromStop(stopCh, 5*time.Second)
		crd, err := c.apiext.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, kplanev1.FleetCRDName, metav1.GetOptions{})
		cancel()
		if err == nil {
			for _, cond := range crd.Status.Conditions {
				if cond.Type == apiextensionsv1.Established && cond.Status == apiextensionsv1.ConditionTrue {
					return nil
				}
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %s", fleetCRDPollTotal)
		}
		time.Sleep(fleetCRDPollEvery)
	}
}

func (c *FleetController) onFleetAdd(obj interface{})            { c.enqueue(obj) }
func (c *FleetController) onFleetUpdate(_, obj interface{})      { c.enqueue(obj) }
func (c *FleetController) onFleetDelete(obj interface{}) {
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = d.Obj
	}
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	name := u.GetName()
	c.mu.Lock()
	delete(c.known, name)
	c.mu.Unlock()
	klog.V(2).Infof("mc.fleet observed deletion fleet=%s (V0 does not GC member VCPs)", name)
}

func (c *FleetController) enqueue(obj interface{}) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	name := u.GetName()
	if name == "" {
		return
	}
	c.mu.Lock()
	c.known[name] = struct{}{}
	c.mu.Unlock()
	c.queue.Add(name)
}

func (c *FleetController) runPeriodicResync(stopCh <-chan struct{}) {
	ticker := time.NewTicker(c.opts.ResyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			c.mu.Lock()
			for name := range c.known {
				c.queue.Add(name)
			}
			c.mu.Unlock()
		}
	}
}

func (c *FleetController) runWorker() {
	for {
		key, quit := c.queue.Get()
		if quit {
			return
		}
		func() {
			defer c.queue.Done(key)
			if err := c.reconcile(key); err != nil {
				klog.Errorf("mc.fleet reconcile failed fleet=%s err=%v", key, err)
				c.queue.AddRateLimited(key)
				return
			}
			c.queue.Forget(key)
		}()
	}
}

// reconcile is the per-Fleet worker. It computes desired members, primes them
// via EnsureCluster, probes readiness, and updates the Fleet status.
func (c *FleetController) reconcile(name string) error {
	gvr := schema.GroupVersionResource{
		Group:    kplanev1.GroupName,
		Version:  kplanev1.Version,
		Resource: "fleets",
	}
	ctx, cancel := contextFromStop(c.stopChSnapshot(), 30*time.Second)
	defer cancel()

	obj, err := c.dyn.Resource(gvr).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get fleet: %w", err)
	}

	fleet, err := decodeFleet(obj)
	if err != nil {
		return fmt.Errorf("decode fleet: %w", err)
	}

	desired, derr := desiredMemberIDs(fleet)
	if derr != nil {
		return c.writeStatus(ctx, gvr, fleet, kplanev1.FleetStatus{
			ObservedGeneration: fleet.Generation,
			Members: []kplanev1.FleetMember{{
				ClusterID: "",
				Phase:     kplanev1.FleetMemberFailed,
				Message:   derr.Error(),
				LastTransitionTime: metav1.Now(),
			}},
		})
	}

	// Trigger bootstrap for every desired member. Ensure is idempotent.
	if c.opts.EnsureCluster != nil {
		for _, cid := range desired {
			c.opts.EnsureCluster(cid)
		}
	}

	// Probe readiness with bounded concurrency.
	members := c.probeMembers(ctx, fleet, desired)

	ready := int32(0)
	for _, m := range members {
		if m.Phase == kplanev1.FleetMemberReady {
			ready++
		}
	}

	return c.writeStatus(ctx, gvr, fleet, kplanev1.FleetStatus{
		ObservedGeneration: fleet.Generation,
		ReadyReplicas:      ready,
		Members:            members,
	})
}

func (c *FleetController) probeMembers(ctx context.Context, fleet *kplanev1.Fleet, desired []string) []kplanev1.FleetMember {
	out := make([]kplanev1.FleetMember, len(desired))
	prevByID := map[string]kplanev1.FleetMember{}
	for _, m := range fleet.Status.Members {
		prevByID[m.ClusterID] = m
	}

	type result struct {
		idx int
		m   kplanev1.FleetMember
	}
	results := make(chan result, len(desired))
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup

	for i, cid := range desired {
		wg.Add(1)
		go func(i int, cid string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			phase, msg := c.probeOne(ctx, cid)
			m := kplanev1.FleetMember{
				ClusterID: cid,
				Phase:     phase,
				Message:   msg,
			}
			if prev, ok := prevByID[cid]; ok && prev.Phase == phase {
				m.LastTransitionTime = prev.LastTransitionTime
			} else {
				m.LastTransitionTime = metav1.Now()
			}
			results <- result{idx: i, m: m}
		}(i, cid)
	}
	wg.Wait()
	close(results)

	for r := range results {
		out[r.idx] = r.m
	}
	return out
}

func (c *FleetController) probeOne(ctx context.Context, cid string) (kplanev1.FleetMemberPhase, string) {
	if c.opts.ClientForCluster == nil {
		return kplanev1.FleetMemberPending, "no client factory configured"
	}
	cs, err := c.opts.ClientForCluster(cid)
	if err != nil {
		return kplanev1.FleetMemberPending, fmt.Sprintf("client: %v", err)
	}
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	body, err := cs.Discovery().RESTClient().Get().AbsPath("/readyz").DoRaw(probeCtx)
	if err != nil {
		return kplanev1.FleetMemberPending, fmt.Sprintf("readyz: %v", err)
	}
	if string(body) == "ok" || len(body) == 0 {
		return kplanev1.FleetMemberReady, ""
	}
	return kplanev1.FleetMemberReady, string(body)
}

func (c *FleetController) writeStatus(ctx context.Context, gvr schema.GroupVersionResource, fleet *kplanev1.Fleet, status kplanev1.FleetStatus) error {
	// Build an unstructured patch with only the status subresource fields.
	statusMap, err := runtimeToMap(status)
	if err != nil {
		return fmt.Errorf("encode status: %w", err)
	}
	patch := map[string]interface{}{
		"status": statusMap,
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal status patch: %w", err)
	}

	_, err = c.dyn.Resource(gvr).Patch(
		ctx, fleet.Name, types.MergePatchType, patchBytes,
		metav1.PatchOptions{}, "status",
	)
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// stopChSnapshot returns the current stopCh under lock so the worker can use
// it to derive request contexts.
func (c *FleetController) stopChSnapshot() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stopCh
}

// desiredMemberIDs computes the list of cluster IDs for a Fleet.
func desiredMemberIDs(fleet *kplanev1.Fleet) ([]string, error) {
	if fleet.Spec.Replicas < 0 {
		return nil, fmt.Errorf("spec.replicas must be >= 0 (got %d)", fleet.Spec.Replicas)
	}
	if len(fleet.Spec.Names) > 0 {
		if int32(len(fleet.Spec.Names)) != fleet.Spec.Replicas {
			return nil, fmt.Errorf("spec.names length (%d) must equal spec.replicas (%d)", len(fleet.Spec.Names), fleet.Spec.Replicas)
		}
		out := make([]string, len(fleet.Spec.Names))
		copy(out, fleet.Spec.Names)
		return out, nil
	}
	prefix := fleet.Spec.NamePrefix
	if prefix == "" {
		prefix = fleet.Name + "-"
	}
	out := make([]string, fleet.Spec.Replicas)
	for i := int32(0); i < fleet.Spec.Replicas; i++ {
		out[i] = fmt.Sprintf("%s%04d", prefix, i)
	}
	return out, nil
}

// decodeFleet turns an unstructured Fleet object into our typed struct.
func decodeFleet(u *unstructured.Unstructured) (*kplanev1.Fleet, error) {
	if u == nil {
		return nil, fmt.Errorf("nil object")
	}
	b, err := u.MarshalJSON()
	if err != nil {
		return nil, err
	}
	var f kplanev1.Fleet
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, err
	}
	return &f, nil
}

// runtimeToMap converts any JSON-serializable struct into map[string]any.
func runtimeToMap(v interface{}) (map[string]interface{}, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	out := map[string]interface{}{}
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// contextFromStop derives a context that respects both stopCh and a timeout.
func contextFromStop(stopCh <-chan struct{}, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	if stopCh == nil {
		return ctx, cancel
	}
	go func() {
		select {
		case <-stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}
