package com.yuanzhy.sqldog.server.common;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/8/30
 */
public enum UpdateBranchType {
    /** 不更新 */
    NOOP,
    /** 更新 一般为替换最小值的情况 */
    UPDATE,
    /** 插入 */
    INSERT,
    /** 删除 */
//    DELETE
}
