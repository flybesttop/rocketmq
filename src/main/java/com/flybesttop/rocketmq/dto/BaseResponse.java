package com.flybesttop.rocketmq.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.flybesttop.rocketmq.util.DateFormatterStr;
import com.flybesttop.rocketmq.util.IdUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * 通用返回对象
 *
 * @author sherry
 * @description
 * @date Create in 2019/12/16
 * @modified By:
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BaseResponse<T> implements Serializable {

    /**
     * 每次返回对应唯一一笔id
     */
    @Builder.Default
    private String id = IdUtil.getUUID();
    /**
     * 系统返回时间
     */
    @Builder.Default
    @JsonFormat(pattern = DateFormatterStr.NORMAL)
    private LocalDateTime resTime = LocalDateTime.now();
    @Builder.Default
    private ResultCode code = ResultCode.SUCCESS;
    /**
     * 对当前code的消息描述
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String message;

    public boolean isSuccess() {
        return code == ResultCode.SUCCESS;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private T data;

    public BaseResponse(T data) {
        this.data = data;
        this.id = IdUtil.getUUID();
        this.resTime = LocalDateTime.now();
        this.code = ResultCode.SUCCESS;
    }

    public BaseResponse(T data , ResultCode code, String message) {
        this.data = data;
        this.id = IdUtil.getUUID();
        this.resTime = LocalDateTime.now();
        this.code = code;
        this.message = message;
    }
}
