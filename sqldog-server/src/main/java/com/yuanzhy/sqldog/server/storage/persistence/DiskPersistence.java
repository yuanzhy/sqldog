package com.yuanzhy.sqldog.server.storage.persistence;

import com.yuanzhy.sqldog.core.exception.PersistenceException;
import com.yuanzhy.sqldog.server.core.Codec;
import com.yuanzhy.sqldog.server.core.Persistence;
import com.yuanzhy.sqldog.server.util.ConfigUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author yuanzhy
 * @date 2022/3/30
 */
public class DiskPersistence implements Persistence {

    private final Codec codec;
    private final String rootPath;
    public DiskPersistence(Codec codec) {
        this.codec = codec;
        this.rootPath = ConfigUtil.getDataPath();
    }

    @Override
    public Map<String, Object> read(String relativePath) throws PersistenceException {
        File f = new File(rootPath, relativePath);
        if (!f.exists()) {
            return Collections.emptyMap();
        }
        try {
            String data = FileUtils.readFileToString(f, "UTF-8");
            return codec.decode(data);
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public void write(String relativePath, Map<String, Object> data) throws PersistenceException {
        String output = codec.encode(data);
        try {
            FileUtils.writeStringToFile(new File(rootPath, relativePath), output,"UTF-8");
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public void delete(String relativePath) throws PersistenceException {
        try {
            FileUtils.delete(new File(rootPath, relativePath));
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public List<String> list(String relativePath) throws PersistenceException {
        File[] folders = new File(rootPath, relativePath).listFiles(pathname -> pathname.isDirectory());
        if (ArrayUtils.isEmpty(folders)) {
            return Collections.emptyList();
        }
        return Arrays.stream(folders).map(f -> relativePath + "/" + f.getName()).collect(Collectors.toList());
    }
}
