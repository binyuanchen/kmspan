package org.kmspan.camel;

import java.util.Properties;

import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriParams;
import org.kmspan.core.SpanConstants;

@UriParams
public class KmspanConfiguration {
    @UriParam(label = "producer", defaultValue = KmspanConstants.MODE_OR_DEFAULT)
    private String modeOrDefault = KmspanConstants.MODE_OR_DEFAULT;

    @UriParam(label = "producer", defaultValue = KmspanConstants.SOURCE_TYPE_KAFKA)
    private String sourceType = KmspanConstants.SOURCE_TYPE_KAFKA;
    @UriParam(label = "producer", defaultValue = KmspanConstants.STORAGE_TYPE_ZOOKEEPER)
    private String storageType = KmspanConstants.STORAGE_TYPE_ZOOKEEPER;

    @UriParam(label = "producer", defaultValue = SpanConstants.DEFAULT_SPAN_ZK_QUORUM)
    private String spanZKQuorum = SpanConstants.DEFAULT_SPAN_ZK_QUORUM;
    @UriParam(label = "producer", defaultValue = SpanConstants.DEFAULT_SPAN_ZK_QUORUM)
    private String spanSCBeginZPath = SpanConstants.DEFAULT_SPAN_BEGIN_SC_ZPATH;
    @UriParam(label = "producer", defaultValue = SpanConstants.DEFAULT_SPAN_END_SC_ZPATH)
    private String spanSCEndZPath = SpanConstants.DEFAULT_SPAN_END_SC_ZPATH;
    @UriParam(label = "producer")
    private String spanSCTargetCount;

    public KmspanConfiguration() {
    }

    private static <T> void addPropertyIfNotNull(Properties props, String key, T value) {
        if (value != null) {
            props.put(key, value.toString());
        }
    }

    public Properties createProducerProperties() {
        Properties props = new Properties();
        addPropertyIfNotNull(props, "mode.or.default", getModeOrDefault());
        addPropertyIfNotNull(props, "source.type", getSourceType());
        addPropertyIfNotNull(props, "storage.type", getStorageType());
        addPropertyIfNotNull(props, "span.zookeeper.quorum", getSpanZKQuorum());
        addPropertyIfNotNull(props, "span.begin.zpath", getSpanSCBeginZPath());
        addPropertyIfNotNull(props, "span.end.zpath", getSpanSCEndZPath());
        return props;
    }

    public Properties createConsumerProperties() {
        Properties props = new Properties();
        return props;
    }

    public String getModeOrDefault() {
        return modeOrDefault;
    }

    public void setModeOrDefault(String modeOrDefault) {
        this.modeOrDefault = modeOrDefault;
    }

    public String getSpanSCTargetCount() {
        return spanSCTargetCount;
    }

    public void setSpanSCTargetCount(String spanSCTargetCount) {
        this.spanSCTargetCount = spanSCTargetCount;
    }

    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    public String getStorageType() {
        return storageType;
    }

    public void setStorageType(String storageType) {
        this.storageType = storageType;
    }

    public String getSpanZKQuorum() {
        return spanZKQuorum;
    }

    public void setSpanZKQuorum(String spanZKQuorum) {
        this.spanZKQuorum = spanZKQuorum;
    }

    public String getSpanSCBeginZPath() {
        return spanSCBeginZPath;
    }

    public void setSpanSCBeginZPath(String spanSCBeginZPath) {
        this.spanSCBeginZPath = spanSCBeginZPath;
    }

    public String getSpanSCEndZPath() {
        return spanSCEndZPath;
    }

    public void setSpanSCEndZPath(String spanSCEndZPath) {
        this.spanSCEndZPath = spanSCEndZPath;
    }
}
