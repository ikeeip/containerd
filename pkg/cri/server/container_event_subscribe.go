/*
   Copyright The containerd Authors.

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

package server

import (
	goevents "github.com/docker/go-events"
	"github.com/sirupsen/logrus"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
)

// SubscribeContainerEvent
func (c *criService) SubscribeContainerEvent(req *runtime.SubscribeContainerEventRequest, stream runtime.RuntimeService_SubscribeContainerEventServer) error {
	ctx := stream.Context()

	mutatorFn := func(wl, wa []string) func(event goevents.Event) goevents.Event {
		return func(e goevents.Event) goevents.Event {
			orig, ok := e.(runtime.ContainerEventMessage)
			if !ok {
				logrus.WithField("event", e).Warnf("invalid event encountered %#v; please file a bug", orig)
				// just keep origin event
				return e
			}

			fFn := func(m map[string]string, l []string) map[string]string {
				ret := make(map[string]string)
				ll := len(l)
				if ll == 0 {
					return ret
				}

				if ll == 1 && l[0] == "*" {
					return m
				}

				for _, label := range l {
					value, exist := m[label]
					if exist {
						ret[label] = value
					}
				}
				return ret
			}

			return runtime.ContainerEventMessage{
				EventType:   orig.GetEventType(),
				ContainerId: orig.GetContainerId(),
				SandboxId:   orig.GetSandboxId(),
				Labels:      fFn(orig.Labels, wl),
				Annotations: fFn(orig.Annotations, wa),
			}
		}
	}(req.WithLabels, req.WithAnnotations)

	eventsC, err := c.eventMonitor.exchange.subscribe(ctx, mutatorFn)
	if err != nil {
		return err
	}
	defer c.eventMonitor.exchange.unsubscribe(eventsC)

	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-eventsC.C:
			evt, ok := e.(runtime.ContainerEventMessage)
			if !ok {
				logrus.WithField("event", e).Errorf("invalid event encountered %#v; please file a bug", evt)
				break
			}
			if err := stream.Send(&evt); err != nil {
				return err
			}
		}
	}
}
