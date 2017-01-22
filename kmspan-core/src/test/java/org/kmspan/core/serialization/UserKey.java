package org.kmspan.core.serialization;

import java.util.Map;

public class UserKey {
    private String keyId;
    private Map<String, UserKeyMetaValue> meta;

    public String getKeyId() {
        return keyId;
    }

    public void setKeyId(String keyId) {
        this.keyId = keyId;
    }

    public Map<String, UserKeyMetaValue> getMeta() {
        return meta;
    }

    public void setMeta(Map<String, UserKeyMetaValue> meta) {
        this.meta = meta;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UserKey userKey = (UserKey) o;

        if (keyId != null ? !keyId.equals(userKey.keyId) : userKey.keyId != null) return false;
        return meta != null ? meta.equals(userKey.meta) : userKey.meta == null;

    }

    @Override
    public int hashCode() {
        int result = keyId != null ? keyId.hashCode() : 0;
        result = 31 * result + (meta != null ? meta.hashCode() : 0);
        return result;
    }
}