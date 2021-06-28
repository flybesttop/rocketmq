package com.flybesttop.rocketmq.util;

import java.util.UUID;

/**
 * ID生成器
 * @author sherry
 * @description
 * @date Create in 2019/12/16
 * @modified By:
 */

public class IdUtil {
    /**
     * 获取uuid剔除-
     * @return
     */
    public static String getUUID(){
        return UUID.randomUUID().toString().replace("-","");
    }
}
