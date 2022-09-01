package com.alibaba.datax.plugin.writer.tugraphwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum TuGraphWriterErrorCode implements ErrorCode {
    RUNTIME_EXCEPTION("TuGraphWriter-00", "运行时异常"),
    LOGIN_ERROR("TuGraphWriter-01", "login tugraph 错误"),
    IMPORT_SCHEMA_ERROR("TuGraphWriter-02", "导入schema错误"),
    IMPORT_DATA_ERROR("TuGraphWriter-03", "导入data错误"),
    PARAMETER_ERROR("TuGraphWriter-04", "参数错误");

    private final String code;
    private final String description;

    private TuGraphWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return null;
    }

    @Override
    public String getDescription() {
        return null;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
