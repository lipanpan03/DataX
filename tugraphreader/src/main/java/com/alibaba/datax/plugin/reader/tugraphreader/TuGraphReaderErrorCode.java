package com.alibaba.datax.plugin.reader.tugraphreader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum TuGraphReaderErrorCode implements ErrorCode {
    REQUIRED_VALUE("TuGraphReader-00", "缺失必要的值"),
    ILLEGAL_VALUE("TuGraphReader-01", "值非法"),
    NOT_SUPPORT_TYPE("TuGraphReader-02", "不支持的column类型"),
    LOGIN_ERROR("TuGraphReader-03", "tugraph-db login错误"),
    EXPORT_ERROR("TuGraphReader-04", "tugraph-db export错误");


    private final String code;
    private final String description;

    private TuGraphReaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
