package com.alibaba.datax.plugin.reader.kafkareader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum KafkaReaderErrorCode implements ErrorCode {
    PARAMTER_ERROR("KafkaReader-00", "参数错误"),
    DATA_ERROR("KafkaReader-01", "数据错误");
    private final String code;
    private final String description;

    private KafkaReaderErrorCode(String code, String description) {
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
