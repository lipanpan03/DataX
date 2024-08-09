package com.alibaba.datax.plugin.reader.tugraphreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;

import org.neo4j.driver.internal.value.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import org.neo4j.driver.*;

public class TuGraphReader extends Reader {

	public static class Job extends Reader.Job {
		private Configuration originalConfig;

		@Override
		public void init() {
			this.originalConfig = super.getPluginJobConf();
		}

		@Override
		public void destroy() {}

		@Override
		public List<Configuration> split(int adviceNumber) {
			List<Configuration> configurations = new ArrayList<>();

			for (int i = 0; i < adviceNumber; i++) {
				configurations.add(this.originalConfig.clone());
			}
			return configurations;
		}
	}

	public static class Task extends Reader.Task {
		private static final Logger LOG = LoggerFactory
				.getLogger(Job.class);
		private Configuration readerSliceConfig;
        private Session session;


		@Override
		public void init() {
			this.readerSliceConfig = super.getPluginJobConf();
            String username = readerSliceConfig.getNecessaryValue(Key.USERNAME, TuGraphReaderErrorCode.REQUIRED_VALUE);
            String password = readerSliceConfig.getNecessaryValue(Key.PASSWORD, TuGraphReaderErrorCode.REQUIRED_VALUE);
            String graphName = readerSliceConfig.getNecessaryValue(Key.GRAPH_NAME, TuGraphReaderErrorCode.REQUIRED_VALUE);
            String url = readerSliceConfig.getString(Key.URL);
            Driver driver = GraphDatabase.driver(url, AuthTokens.basic(username, password));
			session = driver.session(SessionConfig.forDatabase(graphName));
		}

		@Override
		public void startRead(RecordSender recordSender) {
			String queryCypher = readerSliceConfig.getString(Key.QUERY_CYPHER);
			LOG.info("Begin to read record by Cypher: {}.", queryCypher);
			Result rs = session.run(queryCypher);
			List<String> columns = rs.keys();
			int columnNumber = columns.size();
			while (rs.hasNext()) {
				org.neo4j.driver.Record rd = rs.next();
				Record record = recordSender.createRecord();
				Column col;
				for (int i = 0; i < columnNumber; i++) {
					Value value = rd.get(i);
					if (value instanceof StringValue) {
						col = new StringColumn(value.asString());
					} else if (value instanceof BooleanValue) {
						col = new BoolColumn(value.asBoolean());
					} else if (value instanceof IntegerValue) {
						col = new LongColumn(value.asInt());
					} else if (value instanceof FloatValue) {
						col = new DoubleColumn(value.asDouble());
					} else if (value instanceof DateValue) {
						col = new DateColumn(new StringColumn(value.asString()).asDate());
					} else {
						throw DataXException.asDataXException(TuGraphReaderErrorCode.EXPORT_ERROR, "failed to export:" + value.asString());
					}
					record.addColumn(col);
					recordSender.sendToWriter(record);
				}
			}
		}

        @Override
		public void destroy() {
		}

	}

}
