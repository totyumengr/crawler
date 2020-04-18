package github.totyumengr.crawler.worker.task;

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
	
	public static class Task {
		
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
		private CountDownLatch countDown;
		
		public NextPage(Task task, CountDownLatch countDown) {
			super();
			this.task = task;
			this.countDown = countDown;
			
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
			Object structData = structDataClient.getMap(Crawlers.PREFIX_EXTRACT_DATA + task.getExtractor()).get(nextPageUrl);
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
					structDataClient.getList(Crawlers.PREFIX_TASK_RELATED_URLS + task.getFromUrl()).add(encordUrl);
				} else {
					// 落地任务结果
					ResultExporter exporter = applicationContext.getBean(task.getLanding(), ResultExporter.class);
					exporter.export(task);
					logger.info("Start exporter on fromUrl={}", task.getFromUrl());
					
					// 通知任务完成
					countDown.countDown();
				}
			}
		}
	}
	
	class CurrentPage implements Runnable {
		
		private Task task;
		private CountDownLatch countDown;
		
		public CurrentPage(Task task, CountDownLatch countDown) {
			super();
			this.task = task;
			this.countDown = countDown;
		}

		@Override
		public void run() {
			
			// 检查抽取完成的结果
			Object structData = structDataClient.getMap(Crawlers.PREFIX_EXTRACT_DATA + task.getExtractor()).get(task.getFromUrl());
			if (structData != null) {
				// 落地任务结果
				ResultExporter exporter = applicationContext.getBean(task.getLanding(), ResultExporter.class);
				exporter.export(task);
				logger.info("Start exporter on fromUrl={}", task.getFromUrl());
				
				countDown.countDown();
			}
		}
	}
	
	public Task submitTask(String url, String template) throws Exception {
		
		// TODO: 后续改为动态配置的模板获取方式，比如Redis
		Resource taskFile = applicationContext.getResource("classpath:" + template);
		InputStream is = taskFile.getInputStream();
		String taskJson = IOUtils.toString(is, "UTF-8");
		logger.info("Start task={}", taskJson);

		// 获得任务配置
		Task task = Crawlers.GSON.fromJson(taskJson, Task.class);
		// 改变任务的fromUrl
		task.setFromUrl(url);
		
		// 第一步：设置Extractor类型
		structDataClient.getMap(Crawlers.EXTRACTOR).put(task.getFromUrl(), task.getExtractor());
		
		// 第二步：设置提取器的规则
		for (Entry<String, String> entry : task.getExtractRules().entrySet()) {
			structDataClient.getMap(entry.getKey()).put(task.getFromUrl(), entry.getValue());
		}
		
		CountDownLatch countDown = new CountDownLatch(1);
		
		ScheduledExecutorService nextPageSE = Executors.newSingleThreadScheduledExecutor();
		ScheduledExecutorService currentPagePageSE = Executors.newSingleThreadScheduledExecutor();
		// 第三步：设置翻页逻辑
		if (task.isPageDown()) {
			nextPageSE.scheduleAtFixedRate(new NextPage(task, countDown),
					initialDelay, period, TimeUnit.SECONDS);
			logger.info("Start nextpage watcher fromUrl={}", task.getFromUrl());
		} else {
			currentPagePageSE.scheduleAtFixedRate(new CurrentPage(task, countDown),
					initialDelay, period, TimeUnit.SECONDS);
		}
		// 第四步：Launch
		structDataClient.getQueue(Crawlers.BACKLOG).add(task.getFromUrl());
		logger.info("Launch task fromUrl={}", task.getFromUrl());
		
		// 当前任务执行完成
		countDown.await();
		nextPageSE.shutdown();
		currentPagePageSE.shutdown();
		
		return task;
	}
}
