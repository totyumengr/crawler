package github.totyumengr.crawler.worker.task;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.google.gson.reflect.TypeToken;

import github.totyumengr.crawler.Crawlers;
import github.totyumengr.crawler.Crawlers.Task;
import github.totyumengr.crawler.worker.story.StoryWorker;

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
			Object structData = structDataClient.getMap(Crawlers.PREFIX_EXTRACT_DATA + task.getExtractor() + task.getStoryName()).get(nextPageUrl);
			if (structData != null) {
				// 获取下一页链接
				Map<String, Object> extractData = Crawlers.GSON.fromJson(structData.toString(),
						new TypeToken<Map<String, Object>>() {}.getType());
				
				// 处理Repost
				String repostUrl = extractData.containsKey(Crawlers.REPOST) ? 
						extractData.get(Crawlers.REPOST).toString() : null;
//				String repostCookie = extractData.containsKey(Crawlers.REPOST_COOKIE) ? 
//						extractData.get(Crawlers.REPOST_COOKIE).toString() : null;
				// TODO: Cancel当前的任务，更新后重新Launch
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
					structDataClient.getList(Crawlers.PREFIX_TASK_RELATED_URLS + task.getFromUrl() + task.getStoryName()).add(nextPageUrl);

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
			Object structData = structDataClient.getMap(Crawlers.PREFIX_EXTRACT_DATA + task.getExtractor() + task.getStoryName()).get(task.getFromUrl());
			if (structData != null) {
				countDown.countDown();
			}
		}
	}
	
	private void doSubmitTask(String url, Task task) {
		
		// 第一步：设置Extractor类型
		structDataClient.getMap(Crawlers.EXTRACTOR + task.getStoryName()).put(url, task.getExtractor());
		
		// 第二步：设置提取器的规则
		for (Entry<String, String> entry : task.getExtractRules().entrySet()) {
			structDataClient.getMap(entry.getKey() + task.getStoryName()).put(url, entry.getValue());
		}
		
		// 增加Cookie设置
		if (task.getCookies() != null) {
			structDataClient.getMap(Crawlers.COOKIES + task.getStoryName()).put(url, Crawlers.GSON.toJson(task.getCookies()));
		}
		
		// 第四步：Launch
		List<String> backlog = new ArrayList<String>(2);
		backlog.add(task.getStoryName());
		backlog.add(url);
		structDataClient.getQueue(Crawlers.BACKLOG).add(Crawlers.GSON.toJson(backlog));
		logger.info("Launch task fromUrl={}", url);
		
		// 记录Trace
		if (task.isTraceLog()) {
			task.setLogUrl(url);
			structDataClient.getList(Crawlers.PREFIX_STORY_TRACE + task.getStoryName()).add(Crawlers.GSON.toJson(task));
		}
	}
	
	public Task submitTask(Task task) throws Exception {
		
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
		
		Object taskData = structDataClient.getMap(StoryWorker.STORY_TASKS).get(template);
		if (taskData == null) {
			throw new IllegalArgumentException("Invaild task name. " + template);
		}
		
		String taskJson = taskData.toString();
		logger.info("Start task={}", taskJson);

		// 获得任务配置
		Task task = Crawlers.GSON.fromJson(taskJson, Task.class);
		// 改变任务的fromUrl
		task.setFromUrl(url);
		task.setStoryName(storyName);
		
		if (task.getEmulator() != null) {
			// 如果是浏览器执行的任务
			return submitEmulatorTask(task);
		} else {
			// HTTP-Jar执行的任务
			return submitTask(task);
		}
	}
	
	public Task submitEmulatorTask(Task task) throws Exception {
		
		CountDownLatch countDown = new CountDownLatch(1);
		
		ScheduledExecutorService emulatorSE = Executors.newSingleThreadScheduledExecutor();
		emulatorSE.scheduleWithFixedDelay(new CurrentPage(task, countDown),
				initialDelay, period, TimeUnit.SECONDS);
		
		// 第一步：设置Extractor类型
		structDataClient.getMap(Crawlers.EXTRACTOR).put(task.getFromUrl(), task.getExtractor());
		
		// 第四步：Launch
		structDataClient.getQueue(Crawlers.EMULATOR_BACKLOG).add(Crawlers.GSON.toJson(task));
		logger.info("Launch emulator task fromUrl={}", task.getFromUrl());
		
		// 记录Trace
		if (task.isTraceLog()) {
			task.setLogUrl(task.getFromUrl());
			structDataClient.getList(Crawlers.PREFIX_STORY_TRACE + task.getStoryName()).add(Crawlers.GSON.toJson(task));
		}
		
		countDown.await();
		emulatorSE.shutdown();
		
		try {
			// 第五步：落地任务结果
			ResultExporter exporter = applicationContext.getBean(task.getLanding(), ResultExporter.class);
			logger.info("Start exporter on fromUrl={}", task.getFromUrl());
			exporter.export(task);
		} catch (Exception e) {
			logger.error("Error when export task={}", task.getFromUrl());
		}
		
		return task;
	}
}
