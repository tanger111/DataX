package com.alibaba.datax.plugin.writer.chdbwriter;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Getter
@Setter
public class ClickhouseWriterConfig {
    private final static Logger LOG = LoggerFactory.getLogger(ClickhouseWriterConfig.class);

    private Configuration originalConfig;
    private String connString;
    private String passwd;

    private String dbName;
    private String tableName;
    private List<String> columns;

    private Integer batchSize;

    public ClickhouseWriterConfig(Configuration dataxConf) {
       originalConfig = dataxConf;
       connString = dataxConf.getString(Key.JDBC_URL);
       passwd = dataxConf.getString(Key.PASSWORD);
       columns = dataxConf.getList(Key.COLUMN, String.class);
       tableName = dataxConf.getString(Key.TABLE);
       batchSize = dataxConf.getInt(Key.BATCH_SIZE);

    }

}
