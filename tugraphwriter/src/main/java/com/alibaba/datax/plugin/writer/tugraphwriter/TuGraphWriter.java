package com.alibaba.datax.plugin.writer.tugraphwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.antgroup.tugraph.TuGraphDbRpcClient;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
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
        private String startLabelJson;
        private String endLabelJson;
        private String labelType;
        private int batchNum;
        private List<Property> properties = new ArrayList<>();
        private List<String> urls = new ArrayList<>();

        TuGraphDbRpcClient client;

        @Override
        public void init() {
            config = super.getPluginJobConf();
            username = config.getString(Key.USERNAME);
            password = config.getString(Key.PASSWORD);
            graphname = config.getString(Key.GRAPH_NAME);
            labelType = config.getString(Key.LABEL_TYPE);
            labelName = config.getString(Key.LABEL_NAME);
            if (labelType.equals("EDGE")) {
                Gson gson = new Gson();
                startLabel = config.getMap(Key.START_LABEL, String.class);
                endLabel = config.getMap(Key.END_LABEL, String.class);
                startLabelJson = gson.toJson(startLabel);
                endLabelJson = gson.toJson(endLabel);
            }
            urls = config.getList(Key.URLS, String.class);
            batchNum = config.getInt(Key.BATCH_NUM, 1000);
            if (batchNum <= 0) {
                throw new DataXException(TuGraphWriterErrorCode.PARAMETER_ERROR, "batchNum error");
            }
            String schema;
            String startSchema = null, endSchema = null;
            try {
                if (urls.size() == 1) {
                    client = new TuGraphDbRpcClient(urls.get(0), "admin", "73@TuGraph");
                } else if (urls.size() > 1) {
                    client = new TuGraphDbRpcClient(urls, "admin", "73@TuGraph");
                } else {
                    throw new DataXException(TuGraphWriterErrorCode.PARAMETER_ERROR, "urls size error");
                }
                if (labelType.equals("VERTEX")) {
                    schema = client.callCypherToLeader(String.format("CALL db.getVertexSchema('%s')", labelName), graphname, 10000);
                } else {
                    schema = client.callCypherToLeader(String.format("CALL db.getEdgeSchema('%s')", labelName), graphname, 10000);
                    startSchema = client.callCypherToLeader(String.format("CALL db.getVertexSchema('%s')", startLabel.get("type")), graphname, 10000);
                    endSchema = client.callCypherToLeader(String.format("CALL db.getVertexSchema('%s')", endLabel.get("type")), graphname, 10000);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            Map<String, String> allProperties = new HashMap<>();
            {
                Gson gson = new Gson();
                JsonArray jsonArray = gson.fromJson(schema, JsonArray.class);
                JsonObject jsonObject = jsonArray.get(0).getAsJsonObject().get("schema").getAsJsonObject();
                JsonArray array = jsonObject.get("properties").getAsJsonArray();
                for (JsonElement element : array) {
                    JsonObject obj = element.getAsJsonObject();
                    String name = obj.get("name").getAsString();
                    String type = obj.get("type").getAsString();
                    allProperties.put(name, type);
                }
            }
            if (labelType.equals("EDGE")) {
                for (String s : new String[]{startSchema,endSchema}) {
                    Gson gson = new Gson();
                    JsonArray jsonArray = gson.fromJson(s, JsonArray.class);
                    JsonObject jsonObject = jsonArray.get(0).getAsJsonObject().get("schema").getAsJsonObject();
                    String primary = jsonObject.get("primary").getAsString();
                    JsonArray properties = jsonObject.get("properties").getAsJsonArray();
                    for (JsonElement element : properties) {
                        JsonObject obj = element.getAsJsonObject();
                        String name = obj.get("name").getAsString();
                        String type = obj.get("type").getAsString();
                        if (name.equals(primary)) {
                            allProperties.put(name, type);
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

        private void writeTugraph(JsonArray array) throws Exception {
            String str = array.toString().replace("'", "''");
            if (labelType.equals("VERTEX")) {
                client.callCypherToLeader(String.format("CALL db.upsertVertexByJson('%s','%s')", labelName, str), graphname, 10000);
            } else {
                client.callCypherToLeader(String.format("CALL db.upsertEdgeByJson('%s','%s','%s','%s')",
                        labelName, startLabelJson, endLabelJson, str), graphname, 10000);
            }
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            try {
                StringBuilder builder = new StringBuilder();
                Record record;
                JsonArray array = new JsonArray();
                while ((record = recordReceiver.getFromReader()) != null) {
                    int sourceColNum = record.getColumnNumber();
                    if (sourceColNum != properties.size()) {
                        throw new DataXException(TuGraphWriterErrorCode.PARAMETER_ERROR, "reader and writer columns size does not match!");
                    }
                    JsonObject obj = new JsonObject();
                    for (int i = 0; i < sourceColNum; i++) {
                        Column column = record.getColumn(i);
                        Property pro = properties.get(i);
                        if (pro.type.equals("BOOL")) {
                            obj.addProperty(pro.name, column.asBoolean());
                        } else if (pro.type.startsWith("INT")) {
                            obj.addProperty(pro.name, column.asLong());
                        } else if (pro.type.equals("DOUBLE")) {
                            obj.addProperty(pro.name, column.asDouble());
                        } else if (pro.type.equals("FLOAT")) {
                            obj.addProperty(pro.name, column.asDouble());
                        } else if (pro.type.equals("STRING")) {
                            obj.addProperty(pro.name, column.asString());
                        } else if (pro.type.startsWith("DATE")) {
                            obj.addProperty(pro.name, column.asString());
                        } else {
                            throw new DataXException(TuGraphWriterErrorCode.PARAMETER_ERROR, "Unsupported data type " + pro.type);
                        }
                    }
                    array.add(obj);
                    if (array.size() >= batchNum) {
                        writeTugraph(array);
                        array = new JsonArray();
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
