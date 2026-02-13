/*
Copyright 2018 The Kubernetes Authors.

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

package mutating

import (
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"sync"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/sets"
)

type webhookReinvokeContext struct {
	mu sync.Mutex

	// previouslyInvokedWebhooks stores the names of previously invoked webhooks that should be reinvoked
	// in case of a reinvocation.
	previouslyInvokedWebhooks sets.String
	// reinvoke indicates whether the previously invoked webhooks should be reinvoked.
	reinvoke bool
	// lastWebhookOutput stores the object hash of the last webhook output.
	lastWebhookOutput string
}

func (c *webhookReinvokeContext) RequireReinvokingPreviouslyInvokedPlugins() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reinvoke = true
}

func (c *webhookReinvokeContext) SetLastWebhookInvocationOutput(obj runtime.Object) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastWebhookOutput = objHash(obj)
}

func (c *webhookReinvokeContext) IsOutputChangedSinceLastWebhookInvocation(obj runtime.Object) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return objHash(obj) != c.lastWebhookOutput
}

func (c *webhookReinvokeContext) AddReinvocableWebhookToPreviouslyInvoked(uid string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.previouslyInvokedWebhooks == nil {
		c.previouslyInvokedWebhooks = sets.NewString()
	}
	c.previouslyInvokedWebhooks.Insert(uid)
}

func (c *webhookReinvokeContext) ShouldReinvokeWebhook(uid string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.reinvoke {
		return false
	}
	return c.previouslyInvokedWebhooks.Has(uid)
}

func objHash(obj runtime.Object) string {
	if obj == nil {
		return ""
	}
	v := reflect.ValueOf(obj)
	if v.Kind() == reflect.Pointer && v.IsNil() {
		return ""
	}
	u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return ""
	}
	unstructuredObj := &unstructured.Unstructured{Object: u}
	bs, err := json.Marshal(unstructuredObj)
	if err != nil {
		return ""
	}
	hash := sha256.Sum256(bs)
	return hex.EncodeToString(hash[:])
}
