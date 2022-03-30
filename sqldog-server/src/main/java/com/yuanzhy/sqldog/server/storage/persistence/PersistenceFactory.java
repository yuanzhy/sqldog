package com.yuanzhy.sqldog.server.storage.persistence;

import com.yuanzhy.sqldog.core.exception.PersistenceException;
import com.yuanzhy.sqldog.server.core.Cipher;
import com.yuanzhy.sqldog.server.core.Codec;
import com.yuanzhy.sqldog.server.core.Persistence;
import com.yuanzhy.sqldog.server.util.ConfigUtil;

/**
 * @author yuanzhy
 * @date 2022/3/30
 */
public class PersistenceFactory {
    /**
     *
     * @return
     */
    public static Persistence get() {
        if (ConfigUtil.isDisk()) {
            return Holder.DISK_PERSISTENCE;
        }
//        else if (ConfigUtil.isMemory()) {
//        }
        throw new PersistenceException("Persistence strategy not found");
    }

    static Codec getCodec() {
        String codecConfig = ConfigUtil.getProperty("server.storage.codec");
        if ("json".equals(codecConfig)) {
            return Holder.JSON_CODEC;
        } else if ("serialize".equals(codecConfig)) {
            return Holder.SERIALIZE_CODEC;
        } else {
            throw new PersistenceException(codecConfig + " codec strategy not found");
        }
    }

    static Cipher getCipher() {
        return Holder.CIPHER;
//        String secretConfig = ConfigUtil.getProperty("server.storage.secret");
//        if ("false".equals(secretConfig)) {
//            return "";
//        }
    }

    private static class Holder {
        static final Cipher CIPHER = new NoopCipher();
        static final JsonCodec JSON_CODEC = new JsonCodec(getCipher());
        static final SerializeCodec SERIALIZE_CODEC = new SerializeCodec(getCipher());
        static final DiskPersistence DISK_PERSISTENCE = new DiskPersistence(getCodec());
    }
}
