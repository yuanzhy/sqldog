package com.yuanzhy.sqldog.server.storage.persistence;

import com.yuanzhy.sqldog.core.exception.PersistenceException;
import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.common.model.DataPage;
import com.yuanzhy.sqldog.server.core.Codec;
import com.yuanzhy.sqldog.server.core.Persistence;
import com.yuanzhy.sqldog.server.util.ConfigUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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
    public Map<String, Object> readMeta(String storagePath) throws PersistenceException {
        File f = new File(resolvePath(rootPath, storagePath, StorageConst.META_NAME));
        if (!f.exists()) {
            return Collections.emptyMap();
        }
        try {
            String data = FileUtils.readFileToString(f, StorageConst.CHARSET);
            return codec.decode(data);
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public void writeMeta(String storagePath, Map<String, Object> data) throws PersistenceException {
        String output = codec.encode(data);
        try {
            FileUtils.writeStringToFile(new File(resolvePath(rootPath, storagePath, StorageConst.META_NAME)), output, StorageConst.CHARSET);
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
    public DataPage readData(String tablePath, String fileId, int offset) throws PersistenceException {
        Asserts.hasText(tablePath, "tablePath 不能为空");
        Asserts.hasText(fileId, "fileId 不能为空");
        File file = new File(resolvePath(rootPath, tablePath, StorageConst.TABLE_DATA_PATH, fileId));
        if (!file.exists()) {
            return null;
        }
        long pos = (long)offset * StorageConst.PAGE_SIZE;
        if (pos >= file.length()) { // pos 已经大于文件大小了，取下一个文件
            String nextFileId = String.valueOf(Integer.parseInt(fileId) + 1); // next fileId
            int nextOffset = (int)((pos - file.length()) / offset);
            return readData(tablePath, nextFileId, nextOffset);
        }

        byte[] pageBuf = new byte[StorageConst.PAGE_SIZE];
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(pos);
            int n = raf.read(pageBuf);
            if (n != pageBuf.length) {
                throw new PersistenceException("Illegal data page");
            }
            return new DataPage(fileId, offset, pageBuf);
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    /*
     * 可插入数据的数据页
     * 一般情况是最后一个文件的最后一个数据页
     * 特殊情况是复用已删除的空间（暂未实现 TODO）
     */
    @Override
    public DataPage getInsertableData(String tablePath) {
        File folder = new File(resolvePath(rootPath, tablePath, StorageConst.TABLE_DATA_PATH));
        String[] fileIdArr = folder.list((dir, name) -> !name.contains("_"));
        if (ArrayUtils.isEmpty(fileIdArr)) {
            return new DataPage(StorageConst.TABLE_DEF_FILE_ID, new byte[StorageConst.PAGE_SIZE]);
        }
        String lastFileId = Arrays.stream(fileIdArr).max(Comparator.comparing(Integer::valueOf)).orElse("0");
        File file = new File(folder, lastFileId);
        int offset = (int)(file.length() / StorageConst.PAGE_SIZE - 1);
        DataPage dataPage = readData(tablePath, lastFileId, offset);
        if (dataPage == null) {
            dataPage = new DataPage(StorageConst.TABLE_DEF_FILE_ID, new byte[StorageConst.PAGE_SIZE]);
        }
        return dataPage;
    }
    /*
     * 写入数据页
     * 当文件大于1G，新开一个文件
     * return: 实际写入的数据页信息，一般情况和参数dataPate一致
     *         当文档不存在或大于1G后会生成新的数据页
     */
    @Override
    public DataPage writeData(String tablePath, DataPage dataPage) throws PersistenceException {
        File file = new File(resolvePath(rootPath, tablePath, StorageConst.TABLE_DATA_PATH, dataPage.getFileId()));
        if (!file.exists()) {
            // 不存在，创建一个空的
            file.getParentFile().mkdirs();
            try {
                file.createNewFile();
            } catch (IOException e) {
                throw new PersistenceException(e);
            }
            // newDataPage
            dataPage = new DataPage(dataPage.getFileId(), dataPage.getData());
        } else if (file.length() >= StorageConst.MAX_FILE_SIZE) {
            // 单个文件大于1G, 新申请一个
            String nextFileId = String.valueOf(Integer.parseInt(dataPage.getFileId()) + 1);
            file = new File(resolvePath(rootPath, tablePath, StorageConst.TABLE_DATA_PATH, nextFileId));
            while (file.exists()) { // 存在，再去下一个
                nextFileId = String.valueOf(Integer.parseInt(nextFileId) + 1);
                file = new File(resolvePath(rootPath, tablePath, StorageConst.TABLE_DATA_PATH, nextFileId));
            }
            try {
                file.createNewFile();
            } catch (IOException e) {
                throw new PersistenceException(e);
            }
            // newDataPage
            dataPage = new DataPage(nextFileId, dataPage.getData());
        }
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek((long)dataPage.getOffset() * StorageConst.PAGE_SIZE);
            raf.write(dataPage.getData());
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
        return dataPage;
    }

    @Override
    public String resolvePath(String... paths) {
        if (ArrayUtils.isEmpty(paths)) {
            return "";
        }
        return Arrays.stream(paths).filter(path -> StringUtils.isNotEmpty(path)).collect(Collectors.joining("/"));
    }

    @Override
    public void move(String fromPath, String toPath) throws PersistenceException {
        String dataPath = resolvePath(rootPath, fromPath, StorageConst.TABLE_DATA_PATH);
        String tmpTablePath = resolvePath(rootPath, toPath);
        try {
            FileUtils.moveDirectoryToDirectory(new File(dataPath), new File(tmpTablePath), true);
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }
}
