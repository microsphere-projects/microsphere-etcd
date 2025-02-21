/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.microsphere.etcd.spring.cloud.client.discovery;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.util.CollectionUtils;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static io.microsphere.etcd.spring.cloud.client.util.KVClientUtils.buildInstanceId;
import static io.microsphere.etcd.spring.cloud.client.util.KVClientUtils.buildServicePath;
import static io.microsphere.etcd.spring.cloud.client.util.KVClientUtils.toByteSequence;
import static java.util.Collections.unmodifiableList;

/**
 * Cache {@link ServiceInstance} for {@link EtcdDiscoveryClient} and {@link EtcdReactiveDiscoveryClient}
 *
 * @author <a href="mailto:walklown@gmail.com">Walklown</a>
 * @see EtcdDiscoveryClient
 * @see EtcdReactiveDiscoveryClient
 * @since 1.0.0
 */
public class ServiceInstancesCache {

    private static final Logger logger = LoggerFactory.getLogger(ServiceInstancesCache.class);

    private final ConcurrentMap<String, List<ServiceInstance>> serviceInstancesCache;

    private final String rootPath;

    private final EtcdClientAdapter etcdClientAdapter;

    public ServiceInstancesCache(String rootPath, EtcdClientAdapter etcdClientAdapter) {
        this.serviceInstancesCache = new ConcurrentHashMap<>(16);
        this.rootPath = rootPath;
        this.etcdClientAdapter = etcdClientAdapter;
    }

    public List<ServiceInstance> getValue(String serviceId) {
        // Sync Load in the first time
        // Cache if hit
        // Async update cache based on Watch Events
        List<ServiceInstance> sourceInstanceList = serviceInstancesCache.computeIfAbsent(serviceId, key -> {
            String servicePath = buildServicePath(rootPath, serviceId);
            List<ServiceInstance> serviceInstanceList = doGetInstances(servicePath);
            watchService(servicePath, serviceId);
            return serviceInstanceList;
        });
        return unmodifiableList(sourceInstanceList);
    }

    protected List<ServiceInstance> doGetInstances(String servicePath) {
        ByteSequence key = toByteSequence(servicePath);
        List<KeyValue> keyValues = etcdClientAdapter.getKeyValues(key, false);
        return keyValues.stream()
                .map(KeyValue::getValue)
                .map(etcdClientAdapter::deserializeValue)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private void watchService(String servicePath, String serviceId) {
        ByteSequence key = toByteSequence(servicePath);
        etcdClientAdapter.watchService(key, new ServiceInstanceEventListener(serviceId));
    }

    public class ServiceInstanceEventListener implements Watch.Listener {

        private final String serviceId;

        public ServiceInstanceEventListener(String serviceId) {
            this.serviceId = serviceId;
        }

        @Override
        public void onNext(WatchResponse response) {
            response.getEvents().forEach(event -> {
                logger.info("WatchEvent : " + event);
                WatchEvent.EventType eventType = event.getEventType();
                switch (eventType) {
                    case PUT:
                        addOrUpdateServiceInstance(event, serviceId);
                        break;
                    case DELETE:
                        deleteServiceInstance(event, serviceId);
                        break;
                    default:
                        logger.warn("Unknown Event Type : " + eventType);
                        break;
                }
            });
        }

        @Override
        public void onError(Throwable throwable) {

        }

        @Override
        public void onCompleted() {
            logger.info("onCompleted()");
        }

        private void addOrUpdateServiceInstance(WatchEvent event, String serviceId) {
            KeyValue currentKeyValue = event.getKeyValue();
            ByteSequence key = currentKeyValue.getKey();
            ByteSequence value = currentKeyValue.getValue();
            String instanceId = buildInstanceId(rootPath, serviceId, key.toString());
            ServiceInstance serviceInstance = etcdClientAdapter.deserializeValue(value);
            synchronized (this) { // TODO: Optimization Lock
                List<ServiceInstance> serviceInstances = serviceInstancesCache.computeIfAbsent(serviceId, i -> new LinkedList<>());

                if (isAddServiceInstance(event)) { // Add
                    serviceInstances.add(serviceInstance);
                } else { // Update
                    int index = -1;
                    int size = serviceInstances.size();
                    if (size > 0) {
                        serviceInstances.add(serviceInstance);
                        for (int i = 0; i < size; i++) {
                            ServiceInstance previousServiceInstance = serviceInstances.get(i);
                            if (instanceId.equals(previousServiceInstance.getInstanceId())) {
                                index = i;
                                break;
                            }
                        }
                        if (index > -1) {
                            serviceInstances.set(index, serviceInstance);
                            return;
                        }
                    }
                    serviceInstances.add(serviceInstance);
                }
            }

        }

        private void deleteServiceInstance(WatchEvent event, String serviceId) {
            KeyValue currentKeyValue = event.getKeyValue();
            String instanceId = buildInstanceId(rootPath, serviceId, currentKeyValue.getKey().toString());
            synchronized (this) { // TODO: Optimization Lock
                List<ServiceInstance> serviceInstances = serviceInstancesCache.get(serviceId);
                if (!CollectionUtils.isEmpty(serviceInstances)) {
                    Iterator<ServiceInstance> iterator = serviceInstances.iterator();
                    while (iterator.hasNext()) {
                        ServiceInstance serviceInstance = iterator.next();
                        if (instanceId.equals(serviceInstance.getInstanceId())) {
                            iterator.remove();
                        }
                    }
                }
            }
        }

        private boolean isAddServiceInstance(WatchEvent event) {
            KeyValue previousKeyValue = event.getPrevKV();
            return previousKeyValue == null || previousKeyValue.getKey().isEmpty();
        }
    }
}
