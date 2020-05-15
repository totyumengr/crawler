package github.totyumengr.crawler.worker.story;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.gson.reflect.TypeToken;

import github.totyumengr.crawler.Crawlers;
import github.totyumengr.crawler.Crawlers.Story;
import github.totyumengr.crawler.Crawlers.Task;
import github.totyumengr.crawler.worker.task.TaskWorker;

/**
 * 根据JSON格式的任务描述文件，指定执行计划
 * @author mengran7
 *
 */
@Component
public class StoryWorker {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	private TaskWorker taskWorker;
	
	@Autowired
	private RedissonClient storyDataClient;
	
	@Value("${worker.initialDelay}")
	private int initialDelay;
	@Value("${worker.period}")
	private int period;
	
	@Value("${worker.story.log.ttl}")
	private Integer storyTTL;
	
	@Value("${exporter.story.dir}")
	private String storyExportDir;
	
	@Value("${worker.runner.parallel}")
	private int storyRunnerParallel;
	
	@PostConstruct
	private void init() {
		
		ScheduledExecutorService storyScanner = Executors.newSingleThreadScheduledExecutor();
		ThreadPoolExecutor storyRunner = new ThreadPoolExecutor((storyRunnerParallel / 3), storyRunnerParallel,
                0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(1));
		
		logger.info("Start to watch {}", Crawlers.STORY_FILE_QUEYE);
		storyScanner.scheduleWithFixedDelay(new Runnable() {

			@Override
			public void run() {
				
				Object storyInQueue = null;
				try {
					storyInQueue = storyDataClient.getQueue(Crawlers.STORY_FILE_QUEYE).poll();
					if (storyInQueue != null) {
						storyRunner.submit(new StoryRunner(storyInQueue.toString()));
						// 找到一个Story
						logger.info("Found a story and submit it. {}", storyInQueue);
					}
				} catch (RejectedExecutionException e) {
					storyDataClient.getQueue(Crawlers.STORY_FILE_QUEYE).add(storyInQueue);
					logger.info("Reject and push back to queue {}", storyInQueue);
				} catch (Exception e) {
					logger.error("Error when try to take a story.", e);
				}
			}
		}, initialDelay, period, TimeUnit.SECONDS);
	}
	
	private class StoryRunner implements Runnable {
		
		private String storyJson;
		public StoryRunner(String storyJson) {
			super();
			this.storyJson = storyJson;
		}

		public void run() {
			
			logger.info("Start story={}", storyJson);

			// 获得故事配置
			Story story = Crawlers.GSON.fromJson(storyJson, Story.class);
			
			try {
				// 开始Story
				openStory(story);
				
				// 预处理
				preStory(story);
				
				// 按照顺序执行任务。当前是单线程调度任务。
				int argsSize = story.getArgs().size();
				for (int j = 0; j < argsSize; j++) {
					String url = story.getArgs().get(j);
					Task submittedTask = null;
					for (Map<String, String> task : story.getTasks()) {

						logger.info("{}/{} Start task={}", j, argsSize, task);
						
						if (task.get(Crawlers.TASK_PARAMS).equals(Crawlers.TASK_PARAMS_ARGS)) {
							// 从任务中获取参数，并提交
							logger.info("Submit task={} using template={}", url, task.get(Crawlers.TASK_TEMPLATE));
							submittedTask = taskWorker.submitTask(story.getName(), url, task.get(Crawlers.TASK_TEMPLATE));
							recyclebin(submittedTask);
						}
						if (task.get(Crawlers.TASK_PARAMS).equals(Crawlers.TASK_PARAMS_PIPELINE)) {
							// 从上下文中获取参数，并提交
							Object pipeline = storyDataClient.getMap(story.getName() + Crawlers.STORY_PIPELINE).get(url);
							if (pipeline == null) {
								logger.error("Can not found data from pipeline url={}", url);
							} else {
								List<String> urlList = Crawlers.GSON.fromJson(pipeline.toString(),
										new TypeToken<List<String>>() {}.getType());
								int urlListSize = urlList.size();
								for (int i = 0; i < urlListSize; i++) {
									String pipelineUrl = urlList.get(i);
									logger.info("{}/{} Submit task={} using template={}", i, urlListSize, pipelineUrl, task.get(Crawlers.TASK_TEMPLATE));
									submittedTask = taskWorker.submitTask(story.getName(), pipelineUrl, task.get(Crawlers.TASK_TEMPLATE));
									recyclebin(submittedTask);
								}
							}
						}
					}
				}
			} catch (Exception e) {
				logger.error("Fail to execute task.", e);
			} finally {
				// 执行清理任务
				closeStory(story);
			}
		}
	}
	
	private void recyclebin(Task task) {
		
		if (task.isAnti()) {
			Object anti = storyDataClient.getMap(task.getStoryName() + Crawlers.EXTRACTOR_CONTENT_ANTI_ALERT).get(task.getFromUrl());
			task.setAntiHtml(anti != null ? anti.toString() : "");
			storyDataClient.getListMultimap(Crawlers.RECYCLE_BIN).put(task.getStoryName(), Crawlers.GSON.toJson(task));
			logger.info("Put {} into recycle-bin because anti.", task.getFromUrl());
		}
		if (!task.isEtlDone()) {
			storyDataClient.getListMultimap(Crawlers.RECYCLE_BIN).put(task.getStoryName(), Crawlers.GSON.toJson(task));
			logger.info("Put {} into recycle-bin because timeout.", task.getFromUrl());
		}
	}
	
	public void cleanIntermediateData(Story story) {
		
		try {
			for (String key : Crawlers.clearDataKeys().getLeft()) {
				storyDataClient.getMap(story.getName() + key).clear();
			}
			for (String key : Crawlers.clearDataKeys().getRight()) {
				storyDataClient.getListMultimap(story.getName() + key).clear();
			}
			logger.info("Done... Clean intermidiate data story={}", story.getName());
		} catch (Exception e) {
			logger.error("Error when try to clean intermidiate data story={}", story.getName());
		}
	}
	
	private void openStory(Story story) throws Exception {
		
		File storyFolder = new File(storyExportDir, story.getName());
		if (storyFolder.exists()) {
			FileUtils.deleteDirectory(storyFolder);
		}
		
		// For 重跑的场景
		cleanIntermediateData(story);
	}
	
	private void preStory(Story story) {
		
		if (story.getArgsEL() != null && !story.getArgsEL().isEmpty()) {
			String[] range = story.getArgsEL().split(",");
			int start = Integer.valueOf(range[0]);
			int end = Integer.valueOf(range[1]);
			List<String> args = new ArrayList<String>();
			for (String arg : story.getArgs()) {
				for (int i = start; i <= end; i++) {
					args.add(String.format(arg, i));
				}
			}
			
			// 随机打乱
			Collections.shuffle(args);
			
			logger.info("Reset story args={} using el={}", story.getArgs(), story.getArgsEL());
			story.setArgs(args);
			logger.info("Done...Reset story args={} using el={}", story.getArgs().size(), story.getArgsEL());
		}
	}
	
	private void closeStory(Story story) {
		
		File storyFolder = new File(storyExportDir, story.getName());
		try {
			
			List<Object> storyTrace = storyDataClient.getListMultimap(story.getName() + Crawlers.STORY_TRACE).get(Crawlers.STORY_TRACE);
			if (storyTrace == null) {
				logger.info("Do not clean story={} because cannot found trace info.", story.getName());
				return;
			}
			
			if (!storyFolder.exists()) {
				FileUtils.forceMkdir(storyFolder);
			}
			// 开始写文件
			File storyLogFile = new File(storyFolder, story.getName() + ".log");
			FileUtils.touch(storyLogFile);
			
			List<String> c = new ArrayList<String>();
			// 输出Body部分
			for (Object taskLog : storyTrace) {
				Task task = Crawlers.GSON.fromJson(taskLog.toString(),
						new TypeToken<Task>() {}.getType());
				c.add(task.getLogUrl());
			}
			FileUtils.writeLines(storyLogFile, c, true);
			logger.info("Story={} logging is done...", story.getName());
			
		} catch (IOException e) {
			logger.error("Error when try to logging story={}", story.getName());
		} finally {
			try {
				storyDataClient.getQueue(Crawlers.STORY_FILE_QUEYE_DONE).add(Crawlers.GSON.toJson(story));
				logger.info("Done...{}", story.getName());
				cleanIntermediateData(story);
			} catch (Exception e) {
				logger.error("Error when try to Expire intermidiate data", e);
			}
		}
	}
}
