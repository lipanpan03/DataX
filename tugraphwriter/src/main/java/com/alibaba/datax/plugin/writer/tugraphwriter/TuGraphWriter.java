package com.alibaba.datax.plugin.writer.tugraphwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TuGraphWriter extends Writer {
    private static int HTTP_TIMEOUT_INMILLIONSECONDS = 600000;
    private static final int POOL_SIZE = 10;
    private static final String NEWLINE_FLAG = System.getProperty("line.separator", "\n");

    private static String executeAndGet(CloseableHttpClient httpClient, HttpRequestBase httpRequestBase) throws Exception {
        HttpResponse response;
        String entiStr = "";
        response = httpClient.execute(httpRequestBase);
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            entiStr = EntityUtils.toString(entity, Consts.UTF_8);
        }
        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            System.err.println("请求地址：" + httpRequestBase.getURI() + ", 请求方法：" + httpRequestBase.getMethod()
                    + ",STATUS CODE = " + response.getStatusLine().getStatusCode());
            httpRequestBase.abort();
            throw new Exception("Response Status Code : " + response.getStatusLine().getStatusCode()
                    + ", content : "  + entiStr);
        }
        return entiStr;
    }

    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);
        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            int batctNum = originalConfig.getInt(Key.BATCH_NUM, 0);
            int batctSize = originalConfig.getInt(Key.BATCH_SIZE, 1024 * 1024 * 10);
            if (batctNum <= 0 && batctSize <= 0) {
                throw DataXException.asDataXException(TuGraphWriterErrorCode.PARAMETER_ERROR,
                        "batctCount 或者 batctSize 设置错误，至少一个大于0");
            }
        }

        @Override
        public void prepare() {
            Object schema = originalConfig.get(Key.SCHEMA);
            if (schema == null) {
                return;
            }
            String username = originalConfig.getString(Key.USERNAME);
            String password = originalConfig.getString(Key.PASSWORD);
            String graphname = originalConfig.getString(Key.GRAPH_NAME);

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("schema", schema);
            String schemaDescription = JSON.toJSONString(jsonObject);
            String httpBaseUrl = String.format("http://%s:%d", originalConfig.getString(Key.HOST), originalConfig.getInt(Key.PORT));
            RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(HTTP_TIMEOUT_INMILLIONSECONDS)
                    .setConnectTimeout(HTTP_TIMEOUT_INMILLIONSECONDS).setConnectionRequestTimeout(HTTP_TIMEOUT_INMILLIONSECONDS)
                    .setStaleConnectionCheckEnabled(true).build();
            CloseableHttpClient httpClient = HttpClientBuilder.create().setMaxConnTotal(POOL_SIZE).setMaxConnPerRoute(POOL_SIZE)
                    .setDefaultRequestConfig(requestConfig).build();

            String token;
            // login
            {
                Map<String, String> body = new HashMap<>();
                body.put("user", username);
                body.put("password", password);
                HttpPost httpPost = new HttpPost(httpBaseUrl + "/login");
                httpPost.setHeader("Accept", "application/json");
                httpPost.setHeader("Content-Type", "application/json");
                httpPost.setEntity(new StringEntity(JSON.toJSONString(body), "UTF-8"));
                try {
                    String response = executeAndGet(httpClient, httpPost);
                    token = "Bearer " + JSONObject.parseObject(response).getString("jwt");
                } catch (Exception e) {
                    throw DataXException.asDataXException(TuGraphWriterErrorCode.LOGIN_ERROR, "login失败:", e);
                }
            }
            // import schema
            {
                Map<String, String> body = new HashMap<>();
                body.put("description", schemaDescription);
                HttpPost httpPost = new HttpPost(httpBaseUrl + "/db/" + graphname + "/schema/text");
                httpPost.setHeader("Accept", "application/json");
                httpPost.setHeader("Content-Type", "application/json");
                httpPost.setHeader("Authorization",token);
                httpPost.setEntity(new StringEntity(JSON.toJSONString(body), "UTF-8"));
                try {
                    String response = executeAndGet(httpClient, httpPost);
                    String log = (String)JSONObject.parseObject(response).getOrDefault("log","");
                    LOG.info("{} return log : {}",httpPost.getMethod(), log);
                } catch (Exception e) {
                    throw DataXException.asDataXException(TuGraphWriterErrorCode.IMPORT_SCHEMA_ERROR, "导入schema失败:", e);
                }
            }
        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                writerSplitConfigs.add(this.originalConfig);
            }

            return writerSplitConfigs;
        }
    }
    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);
        private Configuration writerSliceConfig;
        private String importDescription;
        private String httpBaseUrl;
        private String username;
        private String password;
        private String graphname;
        private String token;
        boolean ignore_errors;
        private int batctNum;
        private int batctSize;
        private CloseableHttpClient httpClient;

        @Override
        public void init() {
            writerSliceConfig = super.getPluginJobConf();
            username = writerSliceConfig.getString(Key.USERNAME);
            password = writerSliceConfig.getString(Key.PASSWORD);
            graphname = writerSliceConfig.getString(Key.GRAPH_NAME);
            ignore_errors = writerSliceConfig.getBool(Key.IGNORE_ERRORS, false);
            batctNum = writerSliceConfig.getInt(Key.BATCH_NUM, -1);
            batctSize = writerSliceConfig.getInt(Key.BATCH_SIZE, 1024 * 1024 * 10);

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("files",writerSliceConfig.get(Key.FILES));
            importDescription = JSON.toJSONString(jsonObject);

            httpBaseUrl = String.format("http://%s:%d", writerSliceConfig.getString(Key.HOST), writerSliceConfig.getInt(Key.PORT));
            RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(HTTP_TIMEOUT_INMILLIONSECONDS)
                    .setConnectTimeout(HTTP_TIMEOUT_INMILLIONSECONDS).setConnectionRequestTimeout(HTTP_TIMEOUT_INMILLIONSECONDS)
                    .setStaleConnectionCheckEnabled(true).build();
            httpClient = HttpClientBuilder.create().setMaxConnTotal(POOL_SIZE).setMaxConnPerRoute(POOL_SIZE)
                    .setDefaultRequestConfig(requestConfig).build();
        }

        @Override
        public void prepare() {
            // login
            {
                Map<String, String> body = new HashMap<>();
                body.put("user", username);
                body.put("password", password);
                HttpPost httpPost = new HttpPost(httpBaseUrl + "/login");
                httpPost.setHeader("Accept", "application/json");
                httpPost.setHeader("Content-Type", "application/json");
                httpPost.setEntity(new StringEntity(JSON.toJSONString(body), "UTF-8"));
                try {
                    String response = executeAndGet(httpClient, httpPost);
                    JSONObject jsonObject = JSONObject.parseObject(response);
                    token = "Bearer " + jsonObject.getString("jwt");
                } catch (Exception e) {
                    throw DataXException.asDataXException(TuGraphWriterErrorCode.LOGIN_ERROR, "login失败:", e);
                }
            }
        }

        @Override
        public void destroy() {

        }

        private String recordToJsonArrayString(Record record) {
            int recordLength = record.getColumnNumber();
            if (0 == recordLength) {
                return NEWLINE_FLAG;
            }
            Column column;
            StringBuilder sb = new StringBuilder();
            JSONArray jsonArray = new JSONArray();
            for (int i = 0; i < recordLength; i++) {
                column = record.getColumn(i);
                jsonArray.add(column.asString());
            }
            sb.append(jsonArray.toString());
            sb.append(NEWLINE_FLAG);
            return sb.toString();
        }

        private void tugraphImportText(String data) {
            Map<String, Object> body = new HashMap<>();
            body.put("description", importDescription);
            body.put("data", data);
            body.put("continue_on_error", ignore_errors ? true : false);
            HttpPost httpPost = new HttpPost(httpBaseUrl + "/db/" + graphname + "/import/text");
            httpPost.setHeader("Accept", "application/json");
            httpPost.setHeader("Content-Type", "application/json");
            httpPost.setHeader("Authorization",token);
            httpPost.setEntity(new StringEntity(JSON.toJSONString(body), "UTF-8"));
            try {
                String response = executeAndGet(httpClient, httpPost);
                JSONObject jsonObject = JSONObject.parseObject(response);
                String log = (String)jsonObject.getOrDefault("log","");
                //LOG.info("{} return log : {}",httpPost.getMethod(), log);
            } catch (Exception e) {
                throw DataXException.asDataXException(TuGraphWriterErrorCode.IMPORT_DATA_ERROR, "导入数据失败:", e);
            }
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            try {
                long count = 0;
                StringBuilder builder = new StringBuilder();
                Record record;
                while ((record = recordReceiver.getFromReader()) != null) {
                    count++;
                    builder.append(recordToJsonArrayString(record));
                    if ((batctSize > 0 && builder.length() >= batctSize) ||
                            (batctNum > 0 && count >= batctNum)) {
                        tugraphImportText(builder.toString());
                        builder = new StringBuilder();
                        count = 0;
                    }
                }
                if (builder.length() > 0) {
                    tugraphImportText(builder.toString());
                }
            } catch (Exception e) {
                    throw DataXException.asDataXException(TuGraphWriterErrorCode.RUNTIME_EXCEPTION, e);
            }
        }
    }

}
