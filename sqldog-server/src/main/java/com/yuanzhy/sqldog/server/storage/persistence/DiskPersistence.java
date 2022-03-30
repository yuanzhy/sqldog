package com.yuanzhy.sqldog.server.storage.persistence;

import com.yuanzhy.sqldog.core.exception.PersistenceException;
import com.yuanzhy.sqldog.server.core.Codec;
import com.yuanzhy.sqldog.server.core.Persistence;
import com.yuanzhy.sqldog.server.util.ConfigUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

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
        String dataPath = ConfigUtil.getProperty("server.storage.path", "data");
        if (!dataPath.startsWith("/")) {
            dataPath = new File(ConfigUtil.getJarPath()).getParent() + "/" + dataPath;
            new File(dataPath).mkdirs();
        }
        rootPath = dataPath;
    }

    @Override
    public Map<String, Object> read(String storagePath) throws PersistenceException {
        File f = new File(rootPath, storagePath);
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
    public void write(String storagePath, Map<String, Object> data) throws PersistenceException {
        String output = codec.encode(data);
        try {
            FileUtils.writeStringToFile(new File(rootPath, storagePath), output,"UTF-8");
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public void delete(String storagePath) throws PersistenceException {
        try {
            FileUtils.delete(new File(rootPath, storagePath));
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public List<String> list(String storagePath) throws PersistenceException {
        File[] folders = new File(rootPath, storagePath).listFiles(pathname -> pathname.isDirectory());
        if (ArrayUtils.isEmpty(folders)) {
            return Collections.emptyList();
        }
        return Arrays.stream(folders).map(f -> resolvePath(storagePath, f.getName())).collect(Collectors.toList());
    }

    @Override
    public String resolvePath(String... paths) {
        if (ArrayUtils.isEmpty(paths)) {
            return "";
        }
        return Arrays.stream(paths).filter(path -> StringUtils.isNotEmpty(path)).collect(Collectors.joining("/"));
    }
}
