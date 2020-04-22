package github.totyumengr.crawler.worker.task;

import java.io.InputStream;
import java.util.List;
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
		private List<Map<String, String>> cookies;
		private int pageDownCount;
		// 相当于Bin-log
		private boolean traceLog = true;
		
		public int getPageDownCount() {
			return pageDownCount;
		}
		public void setPageDownCount(int pageDownCount) {
			this.pageDownCount = pageDownCount;
		}
		public List<Map<String, String>> getCookies() {
			return cookies;
		}
		public void setCookies(List<Map<String, String>> cookies) {
			this.cookies = cookies;
		}
		public Map<String, String> getExtractRules() {
			return extractRules;
		}
		public void setExtractRules(Map<String, String> extractRules) {
			this.extractRules = extractRules;
		}
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
		
		public boolean isTraceLog() {
			return traceLog;
		}
		public void setTraceLog(boolean traceLog) {
			this.traceLog = traceLog;
		}

		// ------------------------ 这几个属性不是预定义的，内部处理用
		private String storyName;
		private String logUrl;
		private String repostUrl;
		
		public String getRepostUrl() {
			return repostUrl;
		}
		public void setRepostUrl(String repostUrl) {
			this.repostUrl = repostUrl;
		}
		public String getLogUrl() {
			return logUrl;
		}
		public void setLogUrl(String logUrl) {
			this.logUrl = logUrl;
		}
		public String getStoryName() {
			return storyName;
		}
		public void setStoryName(String storyName) {
			this.storyName = storyName;
		}
	}
	
	class NextPage implements Runnable {
		
		private Task task;
		private String nextPageUrl;
		private CountDownLatch countDown;
		private int pagingCnt;
		
		public NextPage(Task task, CountDownLatch countDown) {
			super();
			this.task = task;
			this.countDown = countDown;
			
			this.nextPageUrl = task.getFromUrl();
			this.pagingCnt = task.getPageDownCount();
		}
		
		@Override
		public void run() {
			
			// 检查抽取完成的结果
			Object structData = structDataClient.getMap(Crawlers.PREFIX_EXTRACT_DATA + task.getExtractor()).get(nextPageUrl);
			if (structData != null) {
				// 获取下一页链接
				Map<String, Object> extractData = Crawlers.GSON.fromJson(structData.toString(),
						new TypeToken<Map<String, Object>>() {}.getType());
				
				// 处理Repost
				String repostUrl = extractData.containsKey(Crawlers.REPOST) ? 
						extractData.get(Crawlers.REPOST).toString() : null;
				String repostCookie = extractData.containsKey(Crawlers.REPOST_COOKIE) ? 
						extractData.get(Crawlers.REPOST_COOKIE).toString() : null;
				// Cancel当前的任务，更新后重新Launch
				if (repostUrl != null && repostUrl.length() > 0) {
					// 更新Cookie
					task.setRepostUrl(repostUrl);
//					task.setFromUrl(repostUrl);
//					List<Map<String, String>> cookies = task.getCookies();
//					if (cookies == null) {
//						cookies = new ArrayList<Map<String, String>>();
//						task.setCookies(cookies);
//					}
					
					// 通知任务完成
					countDown.countDown();
					return;
				}
						
				String nextPageUrl = extractData.containsKey(Crawlers.XPATH_PAGINGBAR_NEXTURL_ELEMENTS) ? 
						extractData.get(Crawlers.XPATH_PAGINGBAR_NEXTURL_ELEMENTS).toString() : null;
				// 有效的链接	
				if (nextPageUrl != null && pagingCnt >= 1) {
					this.nextPageUrl = nextPageUrl;
					// 第三步：记录关联
					structDataClient.getList(Crawlers.PREFIX_TASK_RELATED_URLS + task.getFromUrl()).add(nextPageUrl);

					// 提交任务
					doSubmitTask(nextPageUrl, task);
					
					// 第五步：记录翻页次数
					pagingCnt--;
				} else {
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
				countDown.countDown();
			}
		}
	}
	
	private void doSubmitTask(String url, Task task) {
		
		// 第一步：设置Extractor类型
		structDataClient.getMap(Crawlers.EXTRACTOR).put(url, task.getExtractor());
		
		// 第二步：设置提取器的规则
		for (Entry<String, String> entry : task.getExtractRules().entrySet()) {
			structDataClient.getMap(entry.getKey()).put(url, entry.getValue());
		}
		
		// 增加Cookie设置
		if (task.getCookies() != null) {
			structDataClient.getMap(Crawlers.COOKIES).put(url, Crawlers.GSON.toJson(task.getCookies()));
		}
		
		// 第四步：Launch
		structDataClient.getQueue(Crawlers.BACKLOG).add(url);
		logger.info("Launch task fromUrl={}", url);
		
		// 记录Trace
		if (task.isTraceLog()) {
			task.setLogUrl(url);
			structDataClient.getList(Crawlers.PREFIX_STORY_TRACE + task.getStoryName()).add(Crawlers.GSON.toJson(task));
		}
	}
	
	public Task submitTask(String storyName, Task task) throws Exception {
		
		CountDownLatch countDown = new CountDownLatch(1);
		
		ScheduledExecutorService nextPageSE = Executors.newSingleThreadScheduledExecutor();
		ScheduledExecutorService currentPagePageSE = Executors.newSingleThreadScheduledExecutor();
		// 第三步：设置翻页逻辑
		if (task.isPageDown()) {
			nextPageSE.scheduleWithFixedDelay(new NextPage(task, countDown),
					initialDelay, period, TimeUnit.SECONDS);
			logger.info("Start nextpage watcher fromUrl={}", task.getFromUrl());
		} else {
			currentPagePageSE.scheduleWithFixedDelay(new CurrentPage(task, countDown),
					initialDelay, period, TimeUnit.SECONDS);
		}
		
		try {
			// 为Task做一些设置然后提交
			doSubmitTask(task.getFromUrl(), task);	
		} catch (Exception e) {
			logger.error("Error when submit task={}", task.getFromUrl());
		}
		
		// 当前任务执行完成
		countDown.await();
		
		// 如果是任务正常结束
		if (task.getRepostUrl() == null) {
			try {
				// 第五步：落地任务结果
				ResultExporter exporter = applicationContext.getBean(task.getLanding(), ResultExporter.class);
				logger.info("Start exporter on fromUrl={}", task.getFromUrl());
				exporter.export(task);
			} catch (Exception e) {
				logger.error("Error when export task={}", task.getFromUrl());
			}
		}
		
		nextPageSE.shutdown();
		currentPagePageSE.shutdown();
		
		return task;
	}
	
	public Task submitTask(String storyName, String url, String template) throws Exception {
		
		// TODO: 后续改为动态配置的模板获取方式，比如Redis
		Resource taskFile = applicationContext.getResource("classpath:" + template);
		InputStream is = taskFile.getInputStream();
		String taskJson = IOUtils.toString(is, "UTF-8");
		logger.info("Start task={}", taskJson);

		// 获得任务配置
		Task task = Crawlers.GSON.fromJson(taskJson, Task.class);
		// 改变任务的fromUrl
		task.setFromUrl(url);
		task.setStoryName(storyName);
		
		// Delegate
		return submitTask(storyName, task);
	}
}
