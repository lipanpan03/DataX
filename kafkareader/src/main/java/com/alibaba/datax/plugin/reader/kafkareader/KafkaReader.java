package com.alibaba.datax.plugin.reader.kafkareader;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.json.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

public class KafkaReader extends Reader {
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);
        private Configuration originalConfig = null;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            String topic = this.originalConfig.getString(Key.TOPIC);
            String server = this.originalConfig.getString(Key.SERVER);
            List<String> column = this.originalConfig.getList(Key.COLUMN, String.class);
            if (null == topic) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.PARAMTER_ERROR,
                        "topic 没有设置.");
            }
            if (null == server) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.PARAMTER_ERROR,
                        "server 没有设置.");
            }
            if (null == column) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.PARAMTER_ERROR,
                        "column 没有设置.");
            }
            if (column.isEmpty()) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.PARAMTER_ERROR,
                        "column 不能为空.");
            }
            String beginDateTime = this.originalConfig.getString(Key.BEGIN_DATE_TIME, null);
            if (beginDateTime != null) {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                try {
                    Date date = simpleDateFormat.parse(beginDateTime);
                } catch (ParseException e) {
                    throw DataXException.asDataXException(KafkaReaderErrorCode.PARAMTER_ERROR,
                            "beginDateTime [yyyy-MM-dd HH:mm:ss] 解析错误");
                }
            }
            String endDateTime = this.originalConfig.getString(Key.END_DATE_TIME, null);
            if (endDateTime != null) {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                try {
                    Date date = simpleDateFormat.parse(endDateTime);
                } catch (ParseException e) {
                    throw DataXException.asDataXException(KafkaReaderErrorCode.PARAMTER_ERROR,
                            "endDateTime [yyyy-MM-dd HH:mm:ss] 解析错误");
                }
            }
        }

        @Override
        public void destroy() {
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> configurations = new ArrayList<Configuration>();
            Integer partitions = this.originalConfig.getInt(Key.PARTITIONS, 1);
            for (int i = 0; i < partitions; i++) {
                configurations.add(this.originalConfig.clone());
            }
            return configurations;
        }
    }
    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);
        private Configuration readerSliceConfig;
        private String bootstrapServers;
        private String kafkaTopic;
        private List<String> columns;
        private Map<String, String> kafkaConfig;
        private String beginDateTime;
        private String endDateTime;
        private long beginTimestamp = 0;
        private long endTimestamp = 0;
        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            bootstrapServers = this.readerSliceConfig.getString(Key.SERVER);
            kafkaTopic = this.readerSliceConfig.getString(Key.TOPIC);
            columns = this.readerSliceConfig.getList(Key.COLUMN, String.class);
            kafkaConfig = this.readerSliceConfig.getMap(Key.KAFKA_CONFIG, String.class);
            beginDateTime = this.readerSliceConfig.getString(Key.BEGIN_DATE_TIME, null);
            endDateTime = this.readerSliceConfig.getString(Key.END_DATE_TIME, null);
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                if (beginDateTime != null) {
                    beginTimestamp = simpleDateFormat.parse(beginDateTime).getTime();
                }
                if (endDateTime != null) {
                    endTimestamp = simpleDateFormat.parse(endDateTime).getTime();
                }
            } catch (Exception e) {
                throw DataXException.asDataXException(KafkaReaderErrorCode.PARAMTER_ERROR,
                        "beginDateTime or endDateTime [yyyy-MM-dd HH:mm:ss] 解析错误");
            }
        }

        @Override
        public void destroy() {
        }

        @Override
        public void startRead(RecordSender recordSender) {
            Properties props = new Properties();
            props.put("bootstrap.servers", bootstrapServers);
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.putAll(kafkaConfig);
            props.put("allow.auto.create.topics", "false");
            props.put("enable.auto.commit", "true");
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
            consumer.subscribe(Collections.singletonList(kafkaTopic));
            if (beginTimestamp > 0) {
                Set<TopicPartition> assignment = new HashSet<>();
                while (assignment.size() == 0) {
                    consumer.poll(Duration.ofMillis(100));
                    assignment = consumer.assignment();
                }
                Map<TopicPartition, Long> timestampToSearch = new HashMap<>();
                for (TopicPartition tp : assignment) {
                    timestampToSearch.put(tp, beginTimestamp);
                }
                Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timestampToSearch);
                for(TopicPartition tp: assignment){
                    OffsetAndTimestamp offsetAndTimestamp = offsets.get(tp);
                    if (offsetAndTimestamp != null) {
                        consumer.seek(tp, offsetAndTimestamp.offset());
                    }
                }
            }
            boolean run = true;
            while (run) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(60));
                if (records.isEmpty()) {
                    break;
                }
                for (ConsumerRecord<String, String> record : records) {
                    Record rec = recordSender.createRecord();
                    if (endTimestamp > 0 && record.timestamp() > endTimestamp) {
                        run = false;
                        break;
                    }
                    try {
                        for (String col : columns) {
                            if (col.equals("__key__")) {
                                rec.addColumn(new StringColumn(record.key()));
                            } else if (col.equals("__value__")) {
                                rec.addColumn(new StringColumn(record.value()));
                            } else if (col.equals("__partition__")) {
                                rec.addColumn(new LongColumn(record.partition()));
                            } else if (col.equals("__offset__")) {
                                rec.addColumn(new LongColumn(record.offset()));
                            } else if (col.equals("__timestamp__")) {
                                rec.addColumn(new LongColumn(record.timestamp()));
                            } else {
                                JSONObject obj = new JSONObject(record.value());
                                rec.addColumn(new StringColumn(obj.getString(col)));
                            }
                        }
                    } catch (final Exception e) {
                        getTaskPluginCollector().collectDirtyRecord(rec, e);
                        continue;
                    }
                    recordSender.sendToWriter(rec);
                }
                recordSender.flush();
            }
        }
    }
}