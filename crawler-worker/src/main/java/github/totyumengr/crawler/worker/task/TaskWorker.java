package github.totyumengr.crawler.worker.task;

import java.util.HashMap;
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
import github.totyumengr.crawler.Crawlers.Task.STATUS;

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
	
	@Value("${worker.task.initialDelay}")
	private int initialDelay;
	
	@Value("${worker.task.period}")
	private int period;
	
	@Value("${worker.wait.timeout}")
	private int timeout;
	
	@Value("${worker.runner.anti.pause}")
	private int pause;
	
	@Value("${worker.runner.anti.retry}")
	private int retry;
	
	abstract class PageData implements Runnable {
		public Map<String, String> taskResult = new HashMap<String, String>();
	}
	
	class NextPage extends PageData implements Runnable {
		
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
			
			try {
				// 检查抽取完成的结果
				Object structData = structDataClient.getQueue(task.getStoryName() + Crawlers.EXTRACT_STRUCT_DATA + task.getExtractor() + nextPageUrl).poll();
				if (structData == null) {
					return;
				}
				taskResult.put(nextPageUrl, structData.toString());
				logger.info("Found task result of {}, {}", nextPageUrl, structData);
				
				// 获取下一页链接
				Map<String, Object> extractData = Crawlers.GSON.fromJson(structData.toString(),
						new TypeToken<Map<String, Object>>() {}.getType());
						
				String nextPageUrl = extractData.containsKey(Crawlers.XPATH_PAGINGBAR_NEXTURL_ELEMENTS) ? 
						extractData.get(Crawlers.XPATH_PAGINGBAR_NEXTURL_ELEMENTS).toString() : null;
				// 有效的链接	
				if (nextPageUrl != null && pagingCnt >= 1) {
					this.nextPageUrl = nextPageUrl;
					// 提交任务
					doSubmitTask(nextPageUrl, task);
					// 第五步：记录翻页次数
					pagingCnt--;
				} else {
					task.setEtlDone(true);
					// 通知任务完成
					countDown.countDown();
				}
			} catch (Exception e) {
				logger.info("Error when try to get current page result.", e);
			}
		}
	}
	
	class CurrentPage extends PageData implements Runnable {
		
		private Task task;
		private CountDownLatch countDown;
		
		public CurrentPage(Task task, CountDownLatch countDown) {
			super();
			this.task = task;
			this.countDown = countDown;
		}

		@Override
		public void run() {
			
			try {
				Object structData = structDataClient.getQueue(task.getStoryName()
						+ Crawlers.EXTRACT_STRUCT_DATA + task.getExtractor() + task.getFromUrl()).poll();
				if (structData == null) {
					return;
				}
				logger.info("Found task result of {}", task.getFromUrl());
				taskResult.put(task.getFromUrl(), structData.toString());
				task.setEtlDone(true);
				countDown.countDown();
			} catch (Exception e) {
				logger.info("Error when try to get current page result.", e);
			}
		}
	}
	
	private void doSubmitTask(String url, Task task) {
		
		// 第一步：设置Extractor类型
		structDataClient.getMap(task.getStoryName() + Crawlers.EXTRACTOR).put(url, task.getExtractor());
		
		// 第二步：设置提取器的规则
		for (Entry<String, String> entry : task.getExtractRules().entrySet()) {
			structDataClient.getMap(task.getStoryName() + entry.getKey()).put(url, entry.getValue());
		}
		
		// 第三步：设置Task状态
		task.setStatus(STATUS.SUBMITTED.name());
		
		// 第四步：Launch
		String submitTarget = task.getEmulator() == null ? Crawlers.BACKLOG : Crawlers.EMULATOR_BACKLOG;
		// 不改变原有的Task
		Task forSubmit = Task.deepClone(task);
		forSubmit.setFromUrl(url);
		structDataClient.getQueue(submitTarget).add(Crawlers.GSON.toJson(forSubmit));
		logger.info("Launch task fromUrl={}", url);
		
		// 记录Trace
		if (task.isTraceLog()) {
			structDataClient.getListMultimap(task.getStoryName() + Crawlers.STORY_TRACE).get(task.getFromUrl())
				.add(Crawlers.GSON.toJson(forSubmit));
		}
	}
	
	private Task submitTask(Task task) throws Exception {
		
		CountDownLatch countDown = new CountDownLatch(1);
		
		ScheduledExecutorService nextPageSE = null;
		ScheduledExecutorService currentPageSE = null;
		
		PageData pageData = null;
		// 第三步：设置翻页逻辑
		if (task.isPageDown()) {
			pageData = new NextPage(task, countDown);
			nextPageSE = Executors.newSingleThreadScheduledExecutor();
			nextPageSE.scheduleWithFixedDelay(pageData,
					initialDelay, period, TimeUnit.MILLISECONDS);
			logger.info("Start nextpage watcher fromUrl={}", task.getFromUrl());
		} else {
			currentPageSE = Executors.newSingleThreadScheduledExecutor();
			pageData = new CurrentPage(task, countDown);
			currentPageSE.scheduleWithFixedDelay(pageData,
					initialDelay, period, TimeUnit.MILLISECONDS);
		}
		
		try {
			// 为Task做一些设置然后提交
			doSubmitTask(task.getFromUrl(), task);	
		} catch (Exception e) {
			logger.error("Error when submit task={}", task.getFromUrl());
		}
		
		// 当前任务执行完成
		boolean isDone = countDown.await(timeout, TimeUnit.SECONDS);
		if (!isDone) {
			task.setStatus(STATUS.TIMEOUTED.name());
		} else {
			task.setStatus(STATUS.FETCHED.name());
		}
		// 记录Trace
		if (task.isTraceLog()) {
			structDataClient.getListMultimap(task.getStoryName() + Crawlers.STORY_TRACE).get(task.getFromUrl())
				.add(Crawlers.GSON.toJson(task));
		}
		
		// 判断是否被反抓取
		Object alert = structDataClient.getMap(task.getStoryName() + Crawlers.EXTRACTOR_CONTENT_ANTI_ALERT).get(task.getFromUrl());
		if (alert != null) {
			task.setAnti(true);
			// 记录Trace
			if (task.isTraceLog()) {
				structDataClient.getListMultimap(task.getStoryName() + Crawlers.STORY_TRACE).get(task.getFromUrl())
					.add(Crawlers.GSON.toJson(task));
			}
		} else {
			// 第五步：落地任务结果
			ResultExporter exporter = applicationContext.getBean(task.getLanding(), ResultExporter.class);
			logger.info("Start exporter on fromUrl={}", task.getFromUrl());
			exporter.export(task, pageData.taskResult);
		}
		
		if (nextPageSE != null) {
			nextPageSE.shutdown();
		}
		if (currentPageSE != null) {
			currentPageSE.shutdown();
		}
		
		return task;
	}
	
	public Task submitTask(String storyName, String url, String template) throws Exception {
		
		Object taskData = structDataClient.getMap(Crawlers.STORY_TASKS).get(template);
		if (taskData == null) {
			throw new IllegalArgumentException("Invaild task name. " + template);
		}
		
		String taskJson = taskData.toString();
		Task task;
		int taskRetry = retry;
		do {
			taskRetry--;
			logger.info("Start task={}, and have {} retry times.", taskJson, taskRetry);
			// 获得任务配置
			task = Crawlers.GSON.fromJson(taskJson, Task.class);
			// 改变任务的fromUrl
			task.setFromUrl(url);
			task.setStoryName(storyName);
			// 提交任务
			submitTask(task);
		} while (task.isAnti() && taskRetry > 0 && cleanIntermediateData(task));
		
		return task;
	}
	
	private boolean cleanIntermediateData(Task task) {
		
		try {
			for (String key : Crawlers.clearDataKeys().getLeft()) {
				structDataClient.getMap(task.getStoryName() + key).fastRemove(task.getFromUrl());
			}
			logger.info("Clean intermidiate data task={}", task.getName());
		} catch (Exception e) {
			logger.error("Error when try to clean intermidiate data task={}", task.getName());
		}
		
		return true;
	}
}
