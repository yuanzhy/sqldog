package com.yuanzhy.sqldog.core.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/17
 */
public class SerializeCodec<T> implements Codec<T> {

    @Override
    public byte[] encode(T obj) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        try (ObjectOutputStream os = new ObjectOutputStream(baos)) {
            os.writeObject(obj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return baos.toByteArray();
    }

    @Override
    public T decode(byte[] bytes) {
        ByteArrayInputStream bois = new ByteArrayInputStream(bytes);
        try (ObjectInputStream ois = new ObjectInputStream(bois)) {
            return (T) ois.readObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
