package org.kmspan.camel;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.camel.impl.UriEndpointComponent;

public class KmspanComponent extends UriEndpointComponent {

    public KmspanComponent() {
        super(KmspanEndpoint.class);
    }

    public KmspanComponent(CamelContext context) {
        super(context, KmspanEndpoint.class);
    }

    @Override
    protected KmspanEndpoint createEndpoint(String uri, String remaining, Map<String, Object> params) throws Exception {
        KmspanEndpoint endpoint = new KmspanEndpoint(uri, this);
        String modeOrDefault = remaining.split("\\?")[0];
        if (modeOrDefault != null) {
            endpoint.getConfiguration().setModeOrDefault(modeOrDefault);
        }
        setProperties(endpoint.getConfiguration(), params);
        setProperties(endpoint, params);

        return endpoint;
    }
}
