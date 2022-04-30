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

    String TABLE_INDEX_NAME_SEP = "$$";

    short TABLE_DEF_FILE_ID = 0;
    short LARGE_FIELD_DEF_FILE_ID = TABLE_DEF_FILE_ID;
    short INDEX_DEF_FILE_ID = 0;

    int LARGE_FIELD_THRESHOLD = 1000;
    // ----------------------------------------------
    long MAX_FILE_SIZE = 1024 * 1024 * 1024;
    short PAGE_SIZE = 16 * 1024;

    int EXTENT_PAGE_COUNT = 64;
    int EXTENT_SIZE = EXTENT_PAGE_COUNT * PAGE_SIZE;
    short PAGE_HEADER_SIZE = 16;
    short CHECK_SUM_SIZE = 4;
    short FREE_START_SIZE = 2;
    short FREE_END_SIZE = 2;
    short FREE_START_OFFSET = CHECK_SUM_SIZE;
    short FREE_END_OFFSET = CHECK_SUM_SIZE + FREE_START_SIZE;
    short DATA_START_OFFSET = PAGE_HEADER_SIZE;

    short INDEX_LEVEL_START = 8;
    short INDEX_BRANCH_START = INDEX_LEVEL_START + 1;
    short INDEX_LEAF_START = INDEX_BRANCH_START + 8;

    short INDEX_LEAF_PREV_PAGE = INDEX_BRANCH_START;

    short INDEX_LEAF_NEXT_PAGE = INDEX_LEAF_PREV_PAGE + 4;


}
