package com.alibaba.datax.plugin.writer.chdbwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum ClickhouseSQLWriterErrorCode implements ErrorCode {
    REQUIRED_VALUE("clickhouse-00", "缺少參數"),
    ILLEGAL_VALUE("clickhouse-01", "參數值不合法"),
    CONN_ERROR("clickhouse-02", "clickhouse jdbc 连接失败")
    ;


    private final String code;
    private final String desc;

    private ClickhouseSQLWriterErrorCode(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    @Override
    public String getCode() {return this.code;}

    @Override
    public String getDescription() {return this.desc; }

    @Override
    public String toString() {
        return String.format("Code: [%s], Desc:[%s]", this.code, this.desc);
    }
}
