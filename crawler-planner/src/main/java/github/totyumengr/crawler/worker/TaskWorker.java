package github.totyumengr.crawler.worker;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.io.IOUtils;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import com.google.gson.reflect.TypeToken;

import github.totyumengr.crawler.Crawlers;

/**
 * 根据JSON格式的任务描述文件，指定执行计划
 * @author mengran7
 *
 */
@Component
public class TaskWorker {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	private RedissonClient structDataClient;
	
	@Autowired
	private ApplicationContext applicationContext;
	
	@Value("${worker.initialDelay}")
	private int initialDelay;
	@Value("${worker.period}")
	private int period;
	@Value("${worker.task.jsonFileName}")
	private String taskJsonFile;
	
	class Task {
		
		private String name;
		private String fromUrl;
		private String extractor;
		private Map<String, String> extractRules;
		private boolean pageDown;
		private String landing;
		private String traceLog;
		
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getFromUrl() {
			return fromUrl;
		}
		public void setFromUrl(String fromUrl) {
			this.fromUrl = fromUrl;
		}
		public String getExtractor() {
			return extractor;
		}
		public void setExtractor(String extractor) {
			this.extractor = extractor;
		}
		public Map<String, String> getExtractRules() {
			return extractRules;
		}
		public void setExtractorRules(Map<String, String> extractRules) {
			this.extractRules = extractRules;
		}
		public boolean isPageDown() {
			return pageDown;
		}
		public void setPageDown(boolean pageDown) {
			this.pageDown = pageDown;
		}
		public String getLanding() {
			return landing;
		}
		public void setLanding(String landing) {
			this.landing = landing;
		}
		public String getTraceLog() {
			return traceLog;
		}
		public void setTraceLog(String traceLog) {
			this.traceLog = traceLog;
		}
	}
	
	class NextPage implements Runnable {
		
		private Task task;
		private String nextPageUrl;
		private URL url;
		
		public NextPage(Task task) {
			super();
			this.task = task;
			this.nextPageUrl = task.getFromUrl();
			try {
				this.url = new URL(this.nextPageUrl);
			} catch (MalformedURLException e) {
				// Ignore
			}
		}
		
		private String prepareUrl(String nextPageUrl) {
			
			if (!nextPageUrl.startsWith(this.nextPageUrl)) {
				nextPageUrl = this.url.getProtocol() + "://" + this.url.getHost()
					+ (this.url.getPort() < 0 ? "/" : this.url.getPort() + "/") + nextPageUrl;
			}
			
			try {
				return URLEncoder.encode(nextPageUrl, "UTF-8");
			} catch (UnsupportedEncodingException e) {
			}
			
			return nextPageUrl;
		}
		
		@Override
		public void run() {
			
			// 检查抽取完成的结果
			Object structData = structDataClient.getMap(Crawlers.EXTRACT_DATA_PREFIX + task.getExtractor()).get(nextPageUrl);
			if (structData != null) {
				// 获取下一页链接
				Map<String, Object> extractData = Crawlers.GSON.fromJson(structData.toString(),
						new TypeToken<Map<String, Object>>() {}.getType());
				String nextPageUrl = extractData.containsKey(Crawlers.XPATH_PAGINGBAR_NEXTURL_ELEMENTS) ? 
						extractData.get(Crawlers.XPATH_PAGINGBAR_NEXTURL_ELEMENTS).toString() : null;
				// 有效的链接	
				if (nextPageUrl != null) {
					
					// 处理成完整的URL，并提交到需求池
					String encordUrl = prepareUrl(nextPageUrl);
					this.nextPageUrl = encordUrl;
					
					// 第一步：设置Extractor类型
					structDataClient.getMap(Crawlers.EXTRACTOR).put(encordUrl, task.getExtractor());
					
					// 第二步：设置提取器的规则
					for (Entry<String, String> entry : task.getExtractRules().entrySet()) {
						structDataClient.getMap(entry.getKey()).put(encordUrl, entry.getValue());
					}
					
					// 第三步：去抓取下一页
					structDataClient.getQueue(Crawlers.BACKLOG).add(encordUrl);
					
					// 第四步：记录关联
					structDataClient.getMap(Crawlers.TASK_RELATED_URLS).put(task.getFromUrl(), encordUrl);
				} else {
					// 所有分页内容都已经抓取完成。
					structDataClient.getQueue(Crawlers.TASK_DONE).add(task.getFromUrl());
				}
			}
		}
	}
	
	@PostConstruct
	private void init() throws Exception {
		
		// TODO: 临时从本地获取，未来改为调度制
		Resource taskFile = applicationContext.getResource("classpath:" + taskJsonFile);
		InputStream is = taskFile.getInputStream();
		String taskJson = IOUtils.toString(is, "UTF-8");
		
		logger.info("Start task={}", taskJson);

		// 获得任务配置
		Task task = Crawlers.GSON.fromJson(taskJson, Task.class);
		
		// 第一步：设置Extractor类型
		structDataClient.getMap(Crawlers.EXTRACTOR).put(task.getFromUrl(), task.getExtractor());
		
		// 第二步：设置提取器的规则
		for (Entry<String, String> entry : task.getExtractRules().entrySet()) {
			structDataClient.getMap(entry.getKey()).put(task.getFromUrl(), entry.getValue());
		}
		
		// 第三步：设置翻页逻辑
		if (task.isPageDown()) {
			Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new NextPage(task),
					initialDelay, period, TimeUnit.SECONDS);
			logger.info("Start nextpage watcher fromUrl={}", task.getFromUrl());
		}
		// 第四步：Launch
		structDataClient.getQueue(Crawlers.BACKLOG).add(task.getFromUrl());
		logger.info("Launch task fromUrl={}", task.getFromUrl());
		
		// 最后：落地任务结果
		TaskResultExporter exporter = applicationContext.getBean(task.getLanding(), TaskResultExporter.class);
		exporter.export(task.getFromUrl());
		logger.info("Start exporter on fromUrl={}", task.getFromUrl());
	}
	
}
