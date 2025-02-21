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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.WatchOption;
import org.springframework.cloud.client.DefaultServiceInstance;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;

/**
 * Adapter {@link Client} for etcd
 *
 * @author <a href="mailto:walklown@gmail.com">Walklown</a>
 * @see Client
 * @since 1.0.0
 */
public class EtcdClientAdapter implements Closeable {

    private final KV kv;

    private final Watch watch;

    private final ObjectMapper objectMapper;

    public EtcdClientAdapter(Client client, ObjectMapper objectMapper) {
        this.kv = client.getKVClient();
        this.watch = client.getWatchClient();
        this.objectMapper = objectMapper;
    }

    public List<KeyValue> getKeyValues(ByteSequence key, boolean isKeysOnly) {
        GetOption.Builder builder = GetOption.newBuilder()
                .withKeysOnly(isKeysOnly)
                .isPrefix(true);
        CompletableFuture<GetResponse> getResponseFuture = kv.get(key, builder.build());
        List<KeyValue> keyValues = emptyList();
        try {
            GetResponse response = getResponseFuture.get(1, TimeUnit.SECONDS);
            keyValues = response.getKvs();
        } catch (Throwable e) {
        }
        return keyValues;
    }

    public DefaultServiceInstance deserializeValue(ByteSequence value) {
        byte[] content = value.getBytes();
        DefaultServiceInstance serviceInstance = null;
        try {
            // FIXME Bug on Jackson
            serviceInstance = objectMapper.readValue(content, DefaultServiceInstance.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return serviceInstance;
    }

    public void watchService(ByteSequence key, Watch.Listener listener) {
        WatchOption.Builder builder = WatchOption.newBuilder().withPrevKV(true).isPrefix(true);
        watch.watch(key, builder.build(), listener);
    }

    @Override
    public void close() {
        this.kv.close();
    }
}
