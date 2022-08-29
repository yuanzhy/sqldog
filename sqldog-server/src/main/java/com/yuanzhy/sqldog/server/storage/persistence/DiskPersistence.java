package com.yuanzhy.sqldog.server.storage.persistence;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;

import com.yuanzhy.sqldog.core.exception.PersistenceException;
import com.yuanzhy.sqldog.core.util.ArrayUtils;
import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.core.util.ByteUtil;
import com.yuanzhy.sqldog.core.util.StringUtils;
import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.common.model.BranchIndexPage;
import com.yuanzhy.sqldog.server.common.model.DataExtent;
import com.yuanzhy.sqldog.server.common.model.DataPage;
import com.yuanzhy.sqldog.server.common.model.IndexPage;
import com.yuanzhy.sqldog.server.common.model.LeafIndexPage;
import com.yuanzhy.sqldog.server.common.model.Page;
import com.yuanzhy.sqldog.server.core.Codec;
import com.yuanzhy.sqldog.server.core.Persistence;
import com.yuanzhy.sqldog.server.util.ConfigUtil;

/**
 * @author yuanzhy
 * @date 2022/3/30
 */
public class DiskPersistence implements Persistence {

    private final Codec codec;
    private final String rootPath;
    public DiskPersistence(Codec codec) {
        this.codec = codec;
        String dataPath = Configs.get().getProperty("sqldog.storage.path", "data");
        if (Paths.get(dataPath).isAbsolute()) {
            new File(dataPath).mkdirs();
        } else {
            dataPath = new File(Configs.get().getJarPath()).getParent() + "/" + dataPath;
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
            File f = new File(rootPath, storagePath);
            if (f.exists()) {
                FileUtils.forceDelete(f);
            }
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
    public DataPage readPage(String tablePath, short fileId, int offset) throws PersistenceException {
        Asserts.hasText(tablePath, "tablePath 不能为空");
        File file = new File(resolvePath(rootPath, tablePath, StorageConst.TABLE_DATA_PATH, fileId));
        if (!file.exists()) {
            return null;
        }
        long pos = (long)offset * StorageConst.PAGE_SIZE;
        if (pos >= file.length()) { // pos 已经大于文件大小了，取下一个文件
            short nextFileId = (short)(fileId + 1); // next fileId
            int nextOffset = (int)((pos - file.length()) / StorageConst.PAGE_SIZE);
            return readPage(tablePath, nextFileId, nextOffset);
        }
        DataPage dataPage = new DataPage(tablePath, fileId, offset);
        read(file, pos, dataPage);
        return dataPage;
    }

    @Override
    public DataExtent readExtent(String tablePath, short fileId, int offset) throws PersistenceException {
        Asserts.hasText(tablePath, "tablePath 不能为空");
        File file = new File(resolvePath(rootPath, tablePath, StorageConst.TABLE_DATA_PATH, fileId));
        if (!file.exists()) {
            return null;
        }
        long pos = (long)offset * StorageConst.EXTENT_SIZE;
        if (pos >= file.length()) { // pos 已经大于文件大小了，取下一个文件
            short nextFileId = (short)(fileId + 1); // next fileId
            int nextOffset = (int)((pos - file.length()) / offset);
            return readExtent(tablePath, nextFileId, nextOffset);
        }
        List<Object> pages = new ArrayList<>();
//        byte[][] pages = new byte[StorageConst.EXTENT_PAGE_COUNT][StorageConst.PAGE_SIZE];
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(pos);
            byte[] page = new byte[StorageConst.PAGE_SIZE];
            while (raf.read(page) == StorageConst.PAGE_SIZE) {
                pages.add(page);
                page = new byte[StorageConst.PAGE_SIZE];
            }
//            for (byte[] page : pages) {
//                if (raf.read(page) < StorageConst.PAGE_SIZE) {
//                    break;
//                }
//            }
            return new DataExtent(fileId, offset, pages.toArray(new byte[0][]));
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
    public DataPage getInsertablePage(String tablePath) {
        File folder = new File(resolvePath(rootPath, tablePath, StorageConst.TABLE_DATA_PATH));
        String[] fileIdArr = folder.list((dir, name) -> !name.contains("_"));
        if (ArrayUtils.isEmpty(fileIdArr)) {
            return new DataPage(tablePath, StorageConst.TABLE_DEF_FILE_ID);
        }
        short lastFileId = Arrays.stream(fileIdArr).map(Short::parseShort).max(Short::compareTo).orElse((short)0);
        File file = new File(folder, String.valueOf(lastFileId));
        int offset = (int)(file.length() / StorageConst.PAGE_SIZE - 1);
        DataPage dataPage = readPage(tablePath, lastFileId, offset);
        if (dataPage == null) {
            dataPage = new DataPage(tablePath, StorageConst.TABLE_DEF_FILE_ID);
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
    public DataPage writePage(final String tablePath, DataPage dataPage) throws PersistenceException {
        File file = new File(resolvePath(rootPath, tablePath, StorageConst.TABLE_DATA_PATH, dataPage.getFileId()));
        if (!file.exists()) {
            // 不存在，创建一个空的
            createFile(file);
            // newDataPage
            dataPage = dataPage.copyTo(dataPage.getFileId());
        } else if (file.length() >= StorageConst.MAX_FILE_SIZE) {
            // 单个文件大于1G, 新申请一个
            short nextFileId = (short)(dataPage.getFileId() + 1);
            file = new File(resolvePath(rootPath, tablePath, StorageConst.TABLE_DATA_PATH, nextFileId));
            while (file.exists()) { // 存在，再去下一个
                ++nextFileId;
                file = new File(resolvePath(rootPath, tablePath, StorageConst.TABLE_DATA_PATH, nextFileId));
            }
            createFile(file);
            // newDataPage
            dataPage = dataPage.copyTo(nextFileId);
        }
        long pos = (long)dataPage.getOffset() * StorageConst.PAGE_SIZE;
        write(file, pos, dataPage);
        return dataPage;
    }

    @Override
    public byte[] readExtraData(String tablePath, byte[] extraId) throws PersistenceException {
        extraId[0] |= 0b01111111; // 抹去第一位
        short fileId = ByteUtil.toShort(extraId); // 定位到文件
        short pageOffset = ByteUtil.toShort(extraId, 2); // 定位到第几页
        short pageCount = ByteUtil.toShort(extraId, 4); // 页的数量, 从0开始
        short lastByteOffset = ByteUtil.toShort(extraId, 6); // 最后一页byte数
        File file = new File(resolvePath(rootPath, tablePath, StorageConst.TABLE_LARGE_FIELD_PATH, fileId));
        Asserts.isTrue(file.exists(), "File not found：" + file.getAbsolutePath());
        byte[] data = new byte[(int)pageCount * StorageConst.PAGE_SIZE + lastByteOffset];
        read(file, (long)pageOffset * StorageConst.PAGE_SIZE, data);
        return data;
    }

    @Override
    public byte[] writeExtraData(String tablePath, byte[] bytes) throws PersistenceException {
        if (bytes.length > StorageConst.MAX_FILE_SIZE) {
            throw new PersistenceException("Extra data size exceed limit: 1GB");
        }
        File folder = new File(resolvePath(rootPath, tablePath, StorageConst.TABLE_LARGE_FIELD_PATH));
        String[] fileIdArr = folder.list((dir, name) -> !name.contains("_"));
        short lastFileId;
        if (ArrayUtils.isEmpty(fileIdArr)) {
            lastFileId = StorageConst.LARGE_FIELD_DEF_FILE_ID;
        } else {
            lastFileId = Arrays.stream(fileIdArr).map(Short::parseShort).max(Short::compareTo).get();
        }
        File file = new File(folder, String.valueOf(lastFileId));
        if (!file.exists()) {
            createFile(file);
        } else if (file.length() + bytes.length > StorageConst.MAX_FILE_SIZE) {
            // 单个文件大于1G, 新申请一个
            ++lastFileId;
            file = new File(folder, String.valueOf(lastFileId));
            while (file.exists()) { // 存在，再去下一个
                ++lastFileId;
                file = new File(folder, String.valueOf(lastFileId));
            }
            createFile(file);
        }
        int fileId = lastFileId;
        if (fileId > 32767) {
            throw new PersistenceException("Extra data file count exceed limit: " + 32767);
        }
        fileId |= 0x1000; // 第一位存为1，代表数据在外部
        byte[] bytes1 = ByteUtil.toBytes((short) fileId); // 定位到文件
        byte[] bytes2 = ByteUtil.toBytes((short)(file.length() / StorageConst.PAGE_SIZE - 1)); // 定位到第几页
        byte[] bytes3 = ByteUtil.toBytes((short) (bytes.length / StorageConst.PAGE_SIZE)); // 页的数量, 从0开始
        byte[] bytes4 = ByteUtil.toBytes((short) (bytes.length % StorageConst.PAGE_SIZE)); // 最后一页byte数
        return new byte[]{bytes1[0], bytes1[1], bytes2[0], bytes2[1], bytes3[0], bytes3[1], bytes4[0], bytes4[1]};
    }

    @Override
    public void writeIndex(String tablePath, String colName, IndexPage indexPage) throws PersistenceException {
        File file = new File(resolvePath(rootPath, tablePath, StorageConst.TABLE_INDEX_PATH, indexFileName(colName, indexPage.getFileId())));
        if (!file.exists()) { // 首次写入索引
            createFile(file);
            indexPage = indexPage.copyTo(indexPage.getFileId());
        } else if (file.length() >= StorageConst.MAX_FILE_SIZE) {
            short nextFileId = (short)(indexPage.getFileId() + 1);
            file = new File(resolvePath(rootPath, tablePath, StorageConst.TABLE_INDEX_PATH, indexFileName(colName, nextFileId)));
            while (file.exists()) { // 存在，再去下一个
                ++nextFileId;
                file = new File(resolvePath(rootPath, tablePath, StorageConst.TABLE_INDEX_PATH, indexFileName(colName, nextFileId)));
            }
            createFile(file);
            indexPage = indexPage.copyTo(nextFileId);
        }
        long pos = indexPage.getOffset()*StorageConst.PAGE_SIZE;
        write(file, pos, indexPage);
    }

    @Override
    public IndexPage newIndex(String tablePath, String colName, int level) throws PersistenceException {
        // 找到最后一页
        File folder = new File(resolvePath(rootPath, tablePath, StorageConst.TABLE_INDEX_PATH));
        String[] fileIdArr = folder.list((dir, name) -> name.startsWith(colName.concat(StorageConst.TABLE_INDEX_NAME_SEP)));
        File lastFile;
        short lastFileId;
        if (ArrayUtils.isEmpty(fileIdArr)) {
            lastFileId = StorageConst.INDEX_DEF_FILE_ID;
            lastFile = new File(folder, indexFileName(colName, lastFileId));
        } else {
            lastFileId = Arrays.stream(fileIdArr).map(n -> Short.parseShort(StringUtils.substringAfterLast(n, StorageConst.TABLE_INDEX_NAME_SEP))).max(Short::compareTo).orElse((short)0);
            lastFile = new File(folder, indexFileName(colName, lastFileId));
        }
        final int offset = (int)(lastFile.length() / StorageConst.PAGE_SIZE);
        IndexPage indexPage = level == 0 ?
                new LeafIndexPage(tablePath, colName, lastFileId, offset) :
                new BranchIndexPage(tablePath, colName, lastFileId, offset).level(level);
        write(lastFile, lastFile.length(), indexPage);
        return indexPage;
    }

    @Override
    public IndexPage getInsertableIndex(String tablePath, String colName, int level) throws PersistenceException {
        // 找到最后一页
        File folder = new File(resolvePath(rootPath, tablePath, StorageConst.TABLE_INDEX_PATH));
        String[] fileIdArr = folder.list((dir, name) -> name.startsWith(colName.concat(StorageConst.TABLE_INDEX_NAME_SEP)));
        File lastFile;
        short lastFileId;
        if (ArrayUtils.isEmpty(fileIdArr)) {
            lastFileId = StorageConst.INDEX_DEF_FILE_ID;
            lastFile = new File(folder, indexFileName(colName, lastFileId));
        } else {
            lastFileId = Arrays.stream(fileIdArr).map(n -> Short.parseShort(StringUtils.substringAfterLast(n, StorageConst.TABLE_INDEX_NAME_SEP))).max(Short::compareTo).orElse((short)0);
            lastFile = new File(folder, indexFileName(colName, lastFileId));
        }
        final int offset = (int)(lastFile.length() / StorageConst.PAGE_SIZE) - 1;
        IndexPage indexPage = level == 0 ?
                new LeafIndexPage(tablePath, colName, lastFileId, offset) :
                new BranchIndexPage(tablePath, colName, lastFileId, offset).level(level);
        write(lastFile, lastFile.length(), indexPage);
        return indexPage;
    }

    @Override
    public IndexPage readIndex(String tablePath, String colName, short fileId, int offset) {
        File indexFile = new File(resolvePath(rootPath, tablePath, StorageConst.TABLE_INDEX_PATH, indexFileName(colName, fileId)));
        if (!indexFile.exists()) {
            return null;
        }
        return read(indexFile, tablePath, colName, fileId, offset);
    }


    @Override
    public LeafIndexPage readLeafIndex(String tablePath, String colName, short fileId, int offset) throws PersistenceException {
        File indexFile = new File(resolvePath(rootPath, tablePath, StorageConst.TABLE_INDEX_PATH, indexFileName(colName, fileId)));
        if (!indexFile.exists()) {
            return null;
        }
        long pos = (long)offset * StorageConst.PAGE_SIZE;
        // 索引的pos是参数给的，不会超出
//        if (pos >= indexFile.length()) { // pos 已经大于文件大小了，取下一个文件
//            String nextFileId = String.valueOf(Integer.parseInt(fileId) + 1); // next fileId
//            int nextOffset = (int)((pos - file.length()) / offset);
//            return readIndex(tablePath, colName, nextFileId, nextOffset);
//        }
        LeafIndexPage indexPage = new LeafIndexPage(tablePath, colName, fileId, offset);
        read(indexFile, pos, indexPage);
        return indexPage;
    }

    @Override
    public String resolvePath(Object... paths) {
        if (ArrayUtils.isEmpty(paths)) {
            return "";
        }
        return Arrays.stream(paths).filter(path -> path != null && !"".equals(path)).map(Object::toString).collect(Collectors.joining("/"));
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

    @Override
    public void writeStatistics(String tablePath, Map<String, Object> data) throws PersistenceException {
        String output = codec.encode(data);
        try {
            FileUtils.writeStringToFile(new File(resolvePath(rootPath, tablePath, StorageConst.STATISTICS_NAME)), output, StorageConst.CHARSET);
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    @Override
    public Map<String, Object> readStatistics(String tablePath) throws PersistenceException {
        File f = new File(resolvePath(rootPath, tablePath, StorageConst.STATISTICS_NAME));
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

    private String indexFileName(String colName, int fileId) {
        return colName + StorageConst.TABLE_INDEX_NAME_SEP + fileId;
    }

    private void createFile(File file) {
        file.getParentFile().mkdirs();
        try {
            file.createNewFile();
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    private void read(File file, long pos, Page page) {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            if (pos > 0) {
                raf.seek(pos);
            }
            page.fillDataFrom(raf);
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    private IndexPage read(File file, String tablePath, String colName, short fileId, int offset) {
        long pos = (long)offset * StorageConst.PAGE_SIZE;
        // 索引的pos是参数给的，不会超出
//        if (pos >= indexFile.length()) { // pos 已经大于文件大小了，取下一个文件
//            String nextFileId = String.valueOf(Integer.parseInt(fileId) + 1); // next fileId
//            int nextOffset = (int)((pos - file.length()) / offset);
//            return readIndex(tablePath, colName, nextFileId, nextOffset);
//        }
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            if (pos > 0) {
                raf.seek(pos);
            }
            return IndexPage.from(raf, tablePath, colName, fileId, offset);
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    @Deprecated
    private void read(File file, long pos, byte[] buf) {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            if (pos > 0) {
                raf.seek(pos);
            }
            int n = raf.read(buf);
            if (buf.length != n) {
                throw new PersistenceException("Illegal extra data, The correct size is " + buf.length + ", in fact is " + n);
            }
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    private void write(File file, long pos, Page... pages) {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            if (pos > 0) {
                raf.seek(pos);
            }
            for (Page page : pages) {
                page.dumpDataTo(raf);
            }
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }
}
