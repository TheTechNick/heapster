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
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/golang/glog"
	"k8s.io/heapster/events/core"
	kube_api "k8s.io/kubernetes/pkg/api"
)

type tcpClient interface {
	Dail() error
	Write([]byte) (int, error)
	Close()
}

type defaultTCPClient struct {
	host string
	conn net.Conn
}

func (t *defaultTCPClient) Dail() error {
	conn, err := net.Dial("tcp", t.host)
	if err != nil {
		return fmt.Errorf("Failed to open tcp connection to '%s': %v", t.host, err)
	}
	t.conn = conn
	return nil
}

func (t *defaultTCPClient) Write(b []byte) (int, error) {
	return t.conn.Write(b)
}

func (t *defaultTCPClient) Close() {
	t.conn.Close()
}

// TCPSink is sending the json representation of a event via tcp
type TCPSink struct {
	client tcpClient
	sync.RWMutex
}

// CreateTCPSink creates a new TCPSink instance
func CreateTCPSink(uri *url.URL) (core.EventSink, error) {
	return &TCPSink{
		client: &defaultTCPClient{
			host: uri.Host,
		},
	}, nil
}

func (s *TCPSink) Name() string {
	return "TCP Sink"
}

func (s *TCPSink) Stop() {
	// Do nothing.
}

func (s *TCPSink) ExportEvents(batch *core.EventBatch) {
	s.Lock()
	defer s.Unlock()
	start := time.Now()

	err := s.client.Dail()
	if err != nil {
		glog.Warningf(err.Error())
		return
	}
	defer s.client.Close()

	for _, event := range batch.Events {
		value, err := getEventValue(event)
		if err != nil {
			glog.Warningf("Failed to convert event to json: %v", err)
			continue
		}

		_, err = s.client.Write(value)
		if err != nil {
			glog.Warningf("Failed to send data to tcp endpoint: %v", err)
		}
	}
	end := time.Now()

	glog.V(4).Infof("Exported %d data to TCP in %s", len(batch.Events), end.Sub(start))
}

// Generate json value for event
func getEventValue(event *kube_api.Event) ([]byte, error) {
	bytes, err := json.Marshal(event)
	if err != nil {
		return bytes, err
	}
	return bytes, nil
}
