package github.totyumengr.crawler.worker.story;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
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
import github.totyumengr.crawler.worker.task.TaskWorker;
import github.totyumengr.crawler.worker.task.TaskWorker.Task;

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
	
	@Autowired
	private ApplicationContext applicationContext;
	
	@Value("${worker.initialDelay}")
	private int initialDelay;
	@Value("${worker.period}")
	private int period;
	@Value("${worker.story.jsonFileName}")
	private String storyJsonFile;
	
	@Value("${worker.story.log.ttl}")
	private Integer storyTTL;
	
	@Value("${exporter.basefilepath}")
	private String fileExporterPath;
	
	class Story {
		
		private String name;
		private List<Map<String, String>> tasks;
		private List<String> args;
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
	}
	
	@PostConstruct
	private void init() throws Exception {
		
		// TODO: 临时从本地获取，未来改为调度制
		Resource storyFile = applicationContext.getResource("classpath:" + storyJsonFile);
		InputStream is = storyFile.getInputStream();
		String storyJson = IOUtils.toString(is, "UTF-8");
		
		logger.info("Start story={}", storyJson);

		// 获得故事配置
		Story story = Crawlers.GSON.fromJson(storyJson, Story.class);
		// 设置日志过期时间
		storyDataClient.getList(Crawlers.PREFIX_STORY_TRACE + story.getName()).expireAsync(storyTTL.longValue(), TimeUnit.DAYS);
		
		// 开始Story
		openStory(story);
		
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
		
		// 执行清理任务
		closeStory(story);
	}
	
	private void openStory(Story story) {
		
		File storyFolder = new File(fileExporterPath, story.getName());
		try {
			if (storyFolder.exists()) {
				FileUtils.deleteDirectory(storyFolder);
			}
		} catch (Exception e) {
			logger.error("Error when try to open story={}", story.getName(), e);
		}
	}
	
	private void closeStory(Story story) {
		
		List<Object> storyTrace = storyDataClient.getList(Crawlers.PREFIX_STORY_TRACE + story.getName());
		if (storyTrace == null) {
			logger.info("Do not clean story={} because cannot found trace info.", story.getName());
			return;
		}
		
		File storyFolder = new File(fileExporterPath, story.getName());
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
			
			storyDataClient.getList(Crawlers.PREFIX_TASK_RELATED_URLS + task.getLogUrl()).clear();
			
			logger.info("Done... Clean intermidiate data url={}", task.getLogUrl());
		} catch (Exception e) {
			logger.error("Error when try to clean intermidiate data url={}", task.getLogUrl());
		}
		
	}
}
