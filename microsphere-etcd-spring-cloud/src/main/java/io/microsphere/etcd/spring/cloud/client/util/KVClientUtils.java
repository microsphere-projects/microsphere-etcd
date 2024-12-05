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
package io.microsphere.etcd.spring.cloud.client.util;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.microsphere.util.StringUtils;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static io.microsphere.util.StringUtils.substringBetween;

/**
 * The utilities class for etcd {@link KV}
 *
 * @author <a href="mailto:mercyblitz@gmail.com">Mercy</a>
 * @see KV
 * @since 1.0.0
 */
public class KVClientUtils {

    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    public static ByteSequence toByteSequence(String value) {
        return ByteSequence.from(value.getBytes(DEFAULT_CHARSET));
    }

    public static String buildServiceInstancePath(String rootPath, String serviceId, String instanceId) {
        return buildServicePath(rootPath, serviceId) + instanceId;
    }

    public static String buildServicePath(String rootPath, String serviceId) {
        return rootPath + "/" + serviceId + "/";
    }

    public static String buildInstanceId(String rootPath, String serviceId, String serviceInstancePath) {
        String servicePath = buildServicePath(rootPath, serviceId);
        return StringUtils.substringAfter(serviceInstancePath, servicePath);
    }

    public static String resolveServiceId(String serviceInstancePath, String rootPath) {
        String prefix = rootPath + "/";
        String end = "/";
        return substringBetween(serviceInstancePath, prefix, end);
    }

}
