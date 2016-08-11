// Copyright 2015 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tcp

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/heapster/events/core"
	kube_api "k8s.io/kubernetes/pkg/api"
	kube_api_unversioned "k8s.io/kubernetes/pkg/api/unversioned"
)

type fakeTCPClient struct {
	dailCalled  bool
	closeCalled bool
	writeOps    int
}

func (f *fakeTCPClient) Dail() error {
	f.dailCalled = true
	return nil
}

func (f *fakeTCPClient) Write(b []byte) (int, error) {
	f.writeOps++
	return len(b), nil
}

func (f *fakeTCPClient) Close() {
	f.closeCalled = true
}

type fakeTCPSink struct {
	core.EventSink
	client *fakeTCPClient
}

func newFakeSink() *fakeTCPSink {
	client := &fakeTCPClient{}
	return &fakeTCPSink{
		&TCPSink{
			client: client,
		}, client,
	}
}

func TestStoreMultipleDataInput(t *testing.T) {
	fakeSink := newFakeSink()
	timestamp := time.Now()

	now := time.Now()
	event1 := kube_api.Event{
		Message:        "event1",
		Count:          100,
		LastTimestamp:  kube_api_unversioned.NewTime(now),
		FirstTimestamp: kube_api_unversioned.NewTime(now),
	}

	event2 := kube_api.Event{
		Message:        "event2",
		Count:          101,
		LastTimestamp:  kube_api_unversioned.NewTime(now),
		FirstTimestamp: kube_api_unversioned.NewTime(now),
	}

	data := core.EventBatch{
		Timestamp: timestamp,
		Events: []*kube_api.Event{
			&event1,
			&event2,
		},
	}

	fakeSink.ExportEvents(&data)
	assert.Equal(t, 2, fakeSink.client.writeOps)
}

func TestCreateTCPSink(t *testing.T) {
	u, _ := url.Parse("localhost:8080")

	//create influxdb sink
	sink, err := CreateTCPSink(u)
	assert.NoError(t, err)

	//check sink name
	assert.Equal(t, sink.Name(), "TCP Sink")
}