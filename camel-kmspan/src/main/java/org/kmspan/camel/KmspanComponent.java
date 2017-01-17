/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kmspan.camel;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.camel.impl.UriEndpointComponent;

public class KmspanComponent extends UriEndpointComponent {

    private ExecutorService workerPool;

    public KmspanComponent() {
        super(KmspanEndpoint.class);
    }

    public KmspanComponent(CamelContext context) {
        super(context, KmspanEndpoint.class);
    }

    @Override
    protected KmspanEndpoint createEndpoint(String uri, String remaining, Map<String, Object> params) throws Exception {
        KmspanEndpoint endpoint = new KmspanEndpoint(uri, this);
        String brokers = remaining.split("\\?")[0];
        if (brokers != null) {
            endpoint.getConfiguration().setBrokers(brokers);
        }

        endpoint.getConfiguration().setWorkerPool(workerPool);

        setProperties(endpoint.getConfiguration(), params);
        setProperties(endpoint, params);

        return endpoint;
    }

    public ExecutorService getWorkerPool() {
        return workerPool;
    }

    public void setWorkerPool(ExecutorService workerPool) {
        this.workerPool = workerPool;
    }

}
