package com.alibaba.datax.plugin.writer.chdbwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

public class ClickhouseWriterTask {
    private final static Logger LOG = LoggerFactory.getLogger(ClickhouseWriterTask.class);

    private ClickhouseWriterConfig config;

    private ClickHouseConnection conn = null;
    private PreparedStatement ps = null;

    private Triple<List<String>, List<Integer>, List<String>> resultSetMetaData;

    private TaskPluginCollector taskPluginCollector = null;
    private int numberColToWrite;

    public ClickhouseWriterTask(ClickhouseWriterConfig config) {
        this.config = config;
//        this.config = new ClickHouseProperties();
//        this.config.setPassword(config.getPasswd());
//        this.config.setDatabase(config.getDbName());
//        this.clickhouseUrl = config.getConnString();

    }

    private void prepare() throws SQLException {
        if (conn == null) {
            ClickHouseProperties pp = new ClickHouseProperties();
            pp.setDatabase(this.config.getDbName());
            pp.setPassword(this.config.getPasswd());
            ClickHouseDataSource ds = new ClickHouseDataSource(this.config.getConnString(), pp);
            conn = ds.getConnection();
            conn.setAutoCommit(false);

//            ps =project-sourceEncoding
        }

    }

    private void setupCol(int pos, int sqlType, Column col) throws SQLException {
        if (col.getRawData()!= null) {
            switch (sqlType) {
//                case :


            }
        }
    }

    private void doBatchInsert(List<Record> records) throws SQLException{
        try {


        } catch(SQLException e) {

        }

    }

    private void close(){

    }

    public void startWriter(RecordReceiver lineReceiver, TaskPluginCollector taskPluginCollector) {
        this.taskPluginCollector = taskPluginCollector;
        Record record;
        try {
            prepare();

            List<Record> buff = Lists.newArrayListWithExpectedSize(config.getBatchSize());
            this.resultSetMetaData = DBUtil.getColumnMetaData(conn, config.getTableName(),
                    StringUtils.join(config.getColumns(), ","));
            while ((record = lineReceiver.getFromReader())!=null) {
                if (record.getColumnNumber() != numberColToWrite) {
                    throw DataXException.asDataXException(ClickhouseSQLWriterErrorCode.ILLEGAL_VALUE, "数据源列数量与配置不一致");
                }
                buff.add(record);
                if (buff.size() >= config.getBatchSize()) {


                }

            }

        } catch (Throwable t){

        } finally {
            close();
        }

    }

    private PreparedStatement createPreparedStatement() throws SQLException {
        StringBuilder sb = new StringBuilder();
        for (String col: config.getColumns()) {
            sb.append("\"");
            sb.append(col);
            sb.append("\"");
            sb.append(",");
        }
        sb.setLength(sb.length()-1);

    }

}
