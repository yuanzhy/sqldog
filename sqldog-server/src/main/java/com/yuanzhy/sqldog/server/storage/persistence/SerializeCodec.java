package com.yuanzhy.sqldog.server.storage.persistence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import com.yuanzhy.sqldog.core.exception.CodecException;
import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.core.Cipher;
import com.yuanzhy.sqldog.server.core.Codec;

/**
 * @author yuanzhy
 * @date 2022/3/30
 */
public class SerializeCodec implements Codec {

    private final Cipher cipher;
    public SerializeCodec(Cipher cipher) {
        this.cipher = cipher;
    }
    @Override
    public String encode(Map<String, Object> data) throws CodecException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        try (ObjectOutputStream os = new ObjectOutputStream(baos)) {
            os.writeObject(data);
        } catch (Exception e) {
            throw new CodecException(e);
        }
        try {
            return cipher.encrypt(baos.toString(StorageConst.CHARSET));
        } catch (UnsupportedEncodingException e) {
            throw new CodecException(e);
        }
    }

    @Override
    public Map<String, Object> decode(String data) throws CodecException {
        ByteArrayInputStream bois = new ByteArrayInputStream(cipher.decrypt(data).getBytes());
        try (ObjectInputStream ois = new ObjectInputStream(bois)) {
            return (Map<String, Object>) ois.readObject();
        } catch (Exception e) {
            throw new CodecException(e);
        }
    }
}
