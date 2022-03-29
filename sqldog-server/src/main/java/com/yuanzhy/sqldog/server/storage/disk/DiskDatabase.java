package com.yuanzhy.sqldog.server.storage.disk;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.core.Database;
import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.storage.builder.SchemaBuilder;
import com.yuanzhy.sqldog.server.storage.memory.MemoryDatabase;
import com.yuanzhy.sqldog.server.util.ConfigUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.IOException;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/3/29
 */
public class DiskDatabase extends MemoryDatabase implements Database {

    public DiskDatabase(String name, String encoding, String description, String tablespace) {
        super(name, encoding, description, tablespace);
        String dataPath = ConfigUtil.getDataPath();
        File folder = new File(dataPath, getName());
        File[] schemaFolders = folder.listFiles(pathname -> pathname.isDirectory());
        if (ArrayUtils.isNotEmpty(schemaFolders)) {
            for (File schemaFolder : schemaFolders) {
                File schemaMetaFile = new File(schemaFolder, StorageConst.META);
                if (schemaMetaFile.exists()) {
                    JSONObject json;
                    try {
                        json = JSON.parseObject(FileUtils.readFileToString(schemaMetaFile, "UTF-8"));
                    } catch (IOException e) {
                        logger.warn(e.getMessage(), e);
                        continue;
                    }
                    String desc = json.getString("description");
                    super.addSchema(new SchemaBuilder().name(schemaFolder.getName()).description(desc).build());
                }
            }
        }
    }

    @Override
    public void addSchema(Schema schema) {

    }

    @Override
    public void dropSchema(String schemaName) {

    }

    @Override
    public void setDescription(String description) {

    }

    @Override
    public void drop() {

    }
}
