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
