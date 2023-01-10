package com.alibaba.datax.plugin.reader.tugraphreader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;

public class TuGraphReader extends Reader {
	private static final int POOL_SIZE = 10;

	private static String executeAndGet(CloseableHttpClient httpClient, HttpRequestBase httpRequestBase) throws Exception {
		HttpResponse response;
		String entiStr = "";
		response = httpClient.execute(httpRequestBase);
		HttpEntity entity = response.getEntity();
		if (entity != null) {
			entiStr = EntityUtils.toString(entity, Consts.UTF_8);
		}
		if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
			System.err.println("url：" + httpRequestBase.getURI() + ", method：" + httpRequestBase.getMethod()
					+ ",status code: " + response.getStatusLine().getStatusCode());
			httpRequestBase.abort();
			throw new Exception("response status code : " + response.getStatusLine().getStatusCode()
					+ ", content : "  + entiStr);
		}
		return entiStr;
	}

	public static class Job extends Reader.Job {
	    private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);
		private Configuration originalConfig;

		@Override
		public void init() {
			this.originalConfig = super.getPluginJobConf();
		}

		@Override
		public void prepare() {
		}

		@Override
		public List<Configuration> split(int adviceNumber) {
			List<Configuration> configurations = new ArrayList<Configuration>();

			for (int i = 0; i < adviceNumber; i++) {
				configurations.add(this.originalConfig.clone());
			}
			return configurations;
		}

		@Override
		public void post() {
		}

		@Override
		public void destroy() {
		}

	}

	public static class Task extends Reader.Task {
		private Configuration readerSliceConfig;
		private String httpBaseUrl;
		private String username;
		private String password;
		private String graphName;
		private String label;
		private boolean isVertex;
		private String srcLabel;
		private String dstLabel;
		private String token;
		private CloseableHttpClient httpClient;
		List<Configuration> columns;


		@Override
		public void init() {
			this.readerSliceConfig = super.getPluginJobConf();
			this.columns = readerSliceConfig.getListConfiguration(Key.COLUMNS);
			if (null == this.columns || this.columns.size() == 0) {
				throw DataXException.asDataXException(TuGraphReaderErrorCode.REQUIRED_VALUE,
						"parameter `columns` is empty or not set");
			}
			this.username = readerSliceConfig.getNecessaryValue(Key.USERNAME, TuGraphReaderErrorCode.REQUIRED_VALUE);
			this.password = readerSliceConfig.getNecessaryValue(Key.PASSWORD, TuGraphReaderErrorCode.REQUIRED_VALUE);
			this.graphName = readerSliceConfig.getNecessaryValue(Key.GRAPH_NAME, TuGraphReaderErrorCode.REQUIRED_VALUE);
			this.httpBaseUrl = String.format("http://%s:%d", readerSliceConfig.getNecessaryValue(Key.HOST, TuGraphReaderErrorCode.REQUIRED_VALUE),
					readerSliceConfig.getNecessaryInt(Key.PORT, TuGraphReaderErrorCode.REQUIRED_VALUE));
			int timeout = 600000;
			RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(timeout)
					.setConnectTimeout(timeout).setConnectionRequestTimeout(timeout)
					.setStaleConnectionCheckEnabled(true).build();
			httpClient = HttpClientBuilder.create().setMaxConnTotal(POOL_SIZE).setMaxConnPerRoute(POOL_SIZE)
					.setDefaultRequestConfig(requestConfig).build();
			this.label = readerSliceConfig.getNecessaryValue(Key.LABEL, TuGraphReaderErrorCode.REQUIRED_VALUE);
			this.isVertex = readerSliceConfig.getNecessaryBool(Key.IS_VERTEX, TuGraphReaderErrorCode.REQUIRED_VALUE);
			if (!this.isVertex) {
				this.srcLabel = readerSliceConfig.getNecessaryValue(Key.SRC_LABEL, TuGraphReaderErrorCode.REQUIRED_VALUE);
				this.dstLabel = readerSliceConfig.getNecessaryValue(Key.DST_LABEL, TuGraphReaderErrorCode.REQUIRED_VALUE);
			}
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
					throw DataXException.asDataXException(TuGraphReaderErrorCode.LOGIN_ERROR, "failed to login", e);
				}
			}
		}

		@Override
		public void startRead(RecordSender recordSender) {
			Map<String, Object> body = new HashMap<>();
			List<String> properties = new ArrayList<>();
			for (Configuration config : this.columns) {
				properties.add(config.getNecessaryValue(Key.NAME,
						TuGraphReaderErrorCode.REQUIRED_VALUE));
			}
			List<Type> types = new ArrayList<>();
			for (Configuration config : this.columns) {
				types.add(Type.valueOf(config.getNecessaryValue(Key.TYPE,
						TuGraphReaderErrorCode.REQUIRED_VALUE).toUpperCase()));
			}
			body.put("label", label);
			body.put("is_vertex", isVertex);
			body.put("properties", properties);
			if (!isVertex) {
				body.put("src_label", this.srcLabel);
				body.put("dst_label", this.dstLabel);
			}
			HttpPost httpPost = new HttpPost(httpBaseUrl + "/db/" + graphName + "/export");
			httpPost.setHeader("Content-Type", "application/json");
			httpPost.setHeader("Authorization",token);
			httpPost.setEntity(new StringEntity(JSON.toJSONString(body), "UTF-8"));
			try {
				HttpResponse response = httpClient.execute(httpPost);
				HttpEntity entity = response.getEntity();
				if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
					System.err.println("url：" + httpPost.getURI() + ", method：" + httpPost.getMethod()
							+ ",status code: " + response.getStatusLine().getStatusCode());
					httpPost.abort();
					throw new Exception("response status code : " + response.getStatusLine().getStatusCode()
							+ ", content : "  + entity.toString());
				}
				if (!entity.isChunked()) {
					throw new Exception("http transfer is not chunked");
				}
				BufferedReader br = new BufferedReader(new InputStreamReader(entity.getContent()));
				String line = null;
				while ((line = br.readLine()) != null) {
					List<String> array = JSONObject.parseArray(line, String.class);
					if (types.size() != array.size()) {
						throw new Exception("line size error, receive " + array.size() + ", expect " + types.size());
					}
					Record record = recordSender.createRecord();
					Column col = null;
					for (int i = 0; i < types.size(); i++) {
						switch (types.get(i)) {
							case STRING:
								col = new StringColumn(array.get(i));
								break;
							case LONG:
								col = new LongColumn(array.get(i));
								break;
							case DOUBLE:
								col = new DoubleColumn(array.get(i));
								break;
							case DATE:
								col = new DateColumn(new StringColumn(array.get(i)).asDate());
								break;
							case BOOL:
								col = new BoolColumn(array.get(i));
								break;
						}
						record.addColumn(col);
					}
					recordSender.sendToWriter(record);
				}
			} catch (Exception e) {
				throw DataXException.asDataXException(TuGraphReaderErrorCode.EXPORT_ERROR, "failed to export:", e);
			}
		}

		@Override
		public void post() {
		}

		@Override
		public void destroy() {
		}

	}

	private enum Type {
		LONG, DOUBLE, STRING, BOOL, DATE;
	}

}
