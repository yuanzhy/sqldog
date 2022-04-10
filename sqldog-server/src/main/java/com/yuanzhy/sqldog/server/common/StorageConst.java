package com.yuanzhy.sqldog.server.common;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/3/29
 */
public interface StorageConst {

    String CHARSET = "UTF-8";

    String META_NAME = ".meta";

    String DEF_DATABASE_NAME = "DEFAULT";

    String DEF_SCHEMA_NAME = "PUBLIC";

    String TABLE_DATA_PATH = "tbd";
    String TABLE_LARGE_FIELD_PATH = "lfd";

    String TABLE_INDEX_PATH = "idx";

    String TABLE_DEF_FILE_ID = "0";
    String LARGE_FIELD_DEF_FILE_ID = TABLE_DEF_FILE_ID;

    int LARGE_FIELD_THRESHOLD = 1000;
    // ----------------------------------------------
    long MAX_FILE_SIZE = 1024 * 1024 * 1024;
    short PAGE_SIZE = 16 * 1024;
    short PAGE_HEADER_SIZE = 16;
    short CHECK_SUM_SIZE = 4;
    short FREE_START_SIZE = 2;
    short FREE_END_SIZE = 2;
    short FREE_START_OFFSET = CHECK_SUM_SIZE;
    short FREE_END_OFFSET = CHECK_SUM_SIZE + FREE_START_SIZE;
    short DATA_START_OFFSET = PAGE_HEADER_SIZE;

}
