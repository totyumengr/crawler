package github.totyumengr.crawler.worker.story;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
	
	public static final String STORY_FILE_QUEYE = "worker.story";
	public static final String STORY_TASKS = "worker.story.tasks";
	
	class Story {
		
		private String name;
		private List<Map<String, String>> tasks;
		private List<String> args;
		private String argsEL;
		
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public List<Map<String, String>> getTasks() {
			return tasks;
		}
		public void setTasks(List<Map<String, String>> tasks) {
			this.tasks = tasks;
		}
		public List<String> getArgs() {
			return args;
		}
		public void setArgs(List<String> args) {
			this.args = args;
		}
		public String getArgsEL() {
			return argsEL;
		}
		public void setArgsEL(String argsEL) {
			this.argsEL = argsEL;
		}
	}
	
	@PostConstruct
	private void init() {
		
		ExecutorService storyScanner = Executors.newSingleThreadExecutor();
		ExecutorService storyRunner = Executors.newFixedThreadPool(storyRunnerParallel);
		
		storyScanner.execute(new Runnable() {

			@Override
			public void run() {
				
				try {
					while (true) {
						Object storyInQueue = storyDataClient.getBlockingQueue(STORY_FILE_QUEYE).take();
						// 找到一个Story
						logger.info("Found a story and submit it. {}", storyInQueue);
						storyRunner.submit(new StoryRunner(storyInQueue.toString()));
					}
				} catch (Exception e) {
					logger.error("Error when try to take a story.", e);
				}
			}
		});
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
				for (String url: story.getArgs()) {
					for (Map<String, String> task : story.getTasks()) {
						
						logger.info("Start task={}", task);
						Task submittedTask = null;
						if (task.get(Crawlers.TASK_PARAMS).equals(Crawlers.TASK_PARAMS_ARGS)) {
							// 从任务中获取参数，并提交
							logger.info("Submit task={} using template={}", url, task.get(Crawlers.TASK_TEMPLATE));
							submittedTask = taskWorker.submitTask(story.getName(), url, task.get(Crawlers.TASK_TEMPLATE));
							// TODO: 当前写这么写，后续要优雅一些。
							while (submittedTask.getRepostUrl() != null) {
								submittedTask.setFromUrl(submittedTask.getRepostUrl());
								submittedTask.setRepostUrl(null);
								logger.info("Repost task={}", submittedTask.getFromUrl());
								submittedTask = taskWorker.submitTask(story.getName(), submittedTask);
							}
						}
						if (task.get(Crawlers.TASK_PARAMS).equals(Crawlers.TASK_PARAMS_PIPELINE)) {
							// 从上下文中获取参数，并提交
							Object pipeline = storyDataClient.getMap(Crawlers.STORY_PIPELINE).get(url);
							if (pipeline == null) {
								logger.error("Can not found data from pipeline url={}", url);
							} else {
								List<String> urlList = Crawlers.GSON.fromJson(pipeline.toString(),
										new TypeToken<List<String>>() {}.getType());
								for (String pipelineUrl: urlList) {
									logger.info("Submit task={} using template={}", pipelineUrl, task.get(Crawlers.TASK_TEMPLATE));
									taskWorker.submitTask(story.getName(), pipelineUrl, task.get(Crawlers.TASK_TEMPLATE));
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
	
	private void openStory(Story story) throws Exception{
		
		File storyFolder = new File(storyExportDir, story.getName());
		if (storyFolder.exists()) {
			FileUtils.deleteDirectory(storyFolder);
		}
		
		storyDataClient.getList(Crawlers.PREFIX_STORY_TRACE + story.getName()).clear();
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
		
		List<Object> storyTrace = storyDataClient.getList(Crawlers.PREFIX_STORY_TRACE + story.getName());
		if (storyTrace == null) {
			logger.info("Do not clean story={} because cannot found trace info.", story.getName());
			return;
		}
		// 设置日志过期时间
		storyDataClient.getList(Crawlers.PREFIX_STORY_TRACE + story.getName())
			.expire(storyTTL.longValue(), TimeUnit.DAYS);
		
		File storyFolder = new File(storyExportDir, story.getName());
		try {
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
				
				// 清除中间数据
				cleanIntermediateData(task);
			}
			FileUtils.writeLines(storyLogFile, c, true);
			logger.info("Story={} logging is done...", story.getName());
			
		} catch (IOException e) {
			logger.error("Error when try to logging story={}", story.getName());
		}
	}
	
	private void cleanIntermediateData(Task task) {
		
		try {
			// TODO: 当前先简单写吧。
			storyDataClient.getMap(Crawlers.PREFIX_EXTRACT_DATA + task.getExtractor()).fastRemoveAsync(task.getLogUrl());
			
			storyDataClient.getMap(Crawlers.XPATH_LIST_ELEMENTS).fastRemoveAsync(task.getLogUrl());
			storyDataClient.getMap(Crawlers.XPATH_RECORD_ELEMENTS).fastRemoveAsync(task.getLogUrl());
			storyDataClient.getMap(Crawlers.XPATH_PAGINGBAR_ELEMENTS).fastRemoveAsync(task.getLogUrl());
			storyDataClient.getMap(Crawlers.XPATH_PAGINGBAR_NEXTURL_ELEMENTS).fastRemoveAsync(task.getLogUrl());
			
			storyDataClient.getMap(Crawlers.XPATH_CONTENT).fastRemoveAsync(task.getLogUrl());
			
			storyDataClient.getMap(Crawlers.EXTRACTOR).fastRemoveAsync(task.getLogUrl());
			
			storyDataClient.getMap(Crawlers.COOKIES).fastRemoveAsync(task.getLogUrl());
			
			storyDataClient.getMap(Crawlers.STORY_PIPELINE).fastRemoveAsync(task.getLogUrl());
			
			storyDataClient.getMap(Crawlers.BACKLOG_REPUSH).fastRemoveAsync(task.getLogUrl());
			
			storyDataClient.getList(Crawlers.PREFIX_TASK_RELATED_URLS + task.getLogUrl()).clear();
			
			// 未定义到Crawlers中。
			storyDataClient.getMap(STORY_TASKS).fastRemoveAsync(task.getName());
			
			logger.info("Done... Clean intermidiate data url={}", task.getLogUrl());
		} catch (Exception e) {
			logger.error("Error when try to clean intermidiate data url={}", task.getLogUrl());
		}
	}
}
