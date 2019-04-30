package com.alibaba.datax.plugin.writer.chdbwriter;

import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Key;

import java.util.List;

public class ClickhouseWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.CHDB;

    public static class Job extends Writer.Job {

        private ClickhouseWriterConfig config;

        @Override
        public void init() {
            config = new ClickhouseWriterConfig(this.getPluginJobConf());
        }

        @Override
        public void prepare() {
            //do nothing

        }

        @Override
        public List<Configuration> split(int adviceNumbser) {
            return null;
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }
    }

    public static class Task extends Writer.Task {
        private ClickhouseWriterConfig config;
        private ClickhouseWriterTask task;

        @Override
        public void init() {
            config = new ClickhouseWriterConfig(this.getPluginJobConf());
            task = new ClickhouseWriterTask(this.config);

        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
        }


        @Override
        public void destroy() {

        }
    }

}
