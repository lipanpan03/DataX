package com.alibaba.datax.plugin.writer.tugraphwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.neo4j.driver.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TuGraphWriter extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);
        private Configuration config;

        @Override
        public void init() {
            this.config = super.getPluginJobConf();
        }

        @Override
        public void destroy() {
            LOG.info("TuGraphWriter Job destroyed");
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                writerSplitConfigs.add(this.config);
            }

            return writerSplitConfigs;
        }
    }
    static class Property {
        private String name;
        private String type;

        public void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }
    }
    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);
        private Configuration config;
        private String username;
        private String password;
        private String graphname;
        private String labelName;
        private Map<String,String> startLabel = new HashMap<>();
        private Map<String,String> endLabel = new HashMap<>();
        private String labelType;
        private int batchNum;
        private List<Property> properties = new ArrayList<>();
        private String url;
        private Driver driver;
        private Session session;

        @Override
        public void init() {
            config = super.getPluginJobConf();
            username = config.getString(Key.USERNAME);
            password = config.getString(Key.PASSWORD);
            graphname = config.getString(Key.GRAPH_NAME);
            labelType = config.getString(Key.LABEL_TYPE);
            labelName = config.getString(Key.LABEL_NAME);
            if (labelType.equals("EDGE")) {
                startLabel = config.getMap(Key.START_LABEL, String.class);
                if (startLabel.get("type") == null || startLabel.get("key") == null) {
                    throw new DataXException(TuGraphWriterErrorCode.PARAMETER_ERROR, "startLabel missing 'type' or 'key'");
                }
                endLabel = config.getMap(Key.END_LABEL, String.class);
                if (endLabel.get("type") == null || endLabel.get("key") == null) {
                    throw new DataXException(TuGraphWriterErrorCode.PARAMETER_ERROR, "endLabel missing 'type' or 'key'");
                }
            }
            url = config.getString(Key.URL);
            batchNum = config.getInt(Key.BATCH_NUM, 1000);
            if (batchNum <= 0) {
                throw new DataXException(TuGraphWriterErrorCode.PARAMETER_ERROR, "batchNum error");
            }
            String schema;
            String startSchema = null, endSchema = null;
            driver = GraphDatabase.driver(url, AuthTokens.basic(username, password));
            session = driver.session(SessionConfig.forDatabase(graphname));
            if (labelType.equals("VERTEX")) {
                Result res = session.run(String.format("CALL db.getVertexSchema('%s')", labelName));
                org.neo4j.driver.Record rec = res.single();
                schema = rec.get("schema").asString();
            } else {
                Result res = session.run(String.format("CALL db.getEdgeSchema('%s')", labelName));
                org.neo4j.driver.Record rec = res.single();
                schema = rec.get("schema").asString();

                res = session.run(String.format("CALL db.getVertexSchema('%s')", startLabel.get("type")));
                rec = res.single();
                startSchema = rec.get("schema").asString();

                res = session.run(String.format("CALL db.getVertexSchema('%s')", endLabel.get("type")));
                rec = res.single();
                endSchema = rec.get("schema").asString();
            }
            Map<String, String> allProperties = new HashMap<>();
            {
                Gson gson = new Gson();
                JsonObject schemaObj = gson.fromJson(schema, JsonObject.class);
                JsonArray array = schemaObj.get("properties").getAsJsonArray();
                for (JsonElement element : array) {
                    JsonObject obj = element.getAsJsonObject();
                    String name = obj.get("name").getAsString();
                    String type = obj.get("type").getAsString();
                    allProperties.put(name, type);
                }
            }
            if (labelType.equals("EDGE")) {
                String[] vertexSchemas = new String[]{startSchema,endSchema};
                for (int i = 0; i < vertexSchemas.length; i++) {
                    Gson gson = new Gson();
                    JsonObject schemaObj = gson.fromJson(vertexSchemas[i], JsonObject.class);
                    String primary = schemaObj.get("primary").getAsString();
                    JsonArray properties = schemaObj.get("properties").getAsJsonArray();
                    for (JsonElement element : properties) {
                        JsonObject obj = element.getAsJsonObject();
                        String name = obj.get("name").getAsString();
                        String type = obj.get("type").getAsString();
                        if (name.equals(primary)) {
                            if (i == 0) {
                                allProperties.put(startLabel.get("key"), type);
                            } else {
                                allProperties.put(endLabel.get("key"), type);
                            }
                            break;
                        }
                    }
                }
            }

            for (String name : config.getList(Key.PROPERTIES, String.class)) {
                Property item = new Property();
                item.setName(name);
                item.setType(allProperties.get(name));
                properties.add(item);
            }
        }

        @Override
        public void destroy() {
            LOG.info("tugraph writer task destroyed.");
        }

        private void writeTugraph(List<Map<String, Value>> array) {
            Result res;
            if (labelType.equals("VERTEX")) {
                Value parameters = Values.parameters("label", labelName, "data", array);
                res = session.run("CALL db.upsertVertex($label,$data)", parameters);
            } else {
                Value parameters = Values.parameters("label", labelName, "startLabel", startLabel, "endLabel", endLabel, "data", array);
                res = session.run("CALL db.upsertEdge($label,$startLabel,$endLabel,$data)", parameters);
            }
            org.neo4j.driver.Record record = res.single();
            int data_error = record.get("data_error").asInt();
            int index_conflict = record.get("index_conflict").asInt();
            if (data_error > 0 || index_conflict > 0) {
                LOG.warn("upsert result: " + record.asMap());
            }
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            try {
                Record record;
                List<Map<String, Value>> array = new ArrayList<>();
                while ((record = recordReceiver.getFromReader()) != null) {
                    int sourceColNum = record.getColumnNumber();
                    if (sourceColNum != properties.size()) {
                        throw new DataXException(TuGraphWriterErrorCode.PARAMETER_ERROR, "reader and writer columns size does not match!");
                    }
                    Map<String, Value> data = new HashMap<>();
                    for (int i = 0; i < sourceColNum; i++) {
                        Column column = record.getColumn(i);
                        Property pro = properties.get(i);
                        if (pro.type.equals("BOOL")) {
                            data.put(pro.name, Values.value(column.asBoolean()));
                        } else if (pro.type.startsWith("INT")) {
                            data.put(pro.name, Values.value(column.asLong()));
                        } else if (pro.type.equals("DOUBLE")) {
                            data.put(pro.name, Values.value(column.asDouble()));
                        } else if (pro.type.equals("FLOAT")) {
                            data.put(pro.name, Values.value(column.asDouble()));
                        } else if (pro.type.equals("STRING")) {
                            data.put(pro.name, Values.value(column.asString()));
                        } else if (pro.type.startsWith("DATE")) {
                            data.put(pro.name, Values.value(column.asString()));
                        } else {
                            throw new DataXException(TuGraphWriterErrorCode.PARAMETER_ERROR, "Unsupported data type " + pro.type);
                        }
                    }
                    array.add(data);
                    if (array.size() >= batchNum) {
                        writeTugraph(array);
                        array = new ArrayList<>();
                    }
                }
                if (array.size() > 0) {
                    writeTugraph(array);
                }
            } catch (Exception e) {
                    throw DataXException.asDataXException(TuGraphWriterErrorCode.RUNTIME_EXCEPTION, e);
            }
        }
    }

}
