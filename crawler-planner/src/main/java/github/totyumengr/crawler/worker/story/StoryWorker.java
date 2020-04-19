package github.totyumengr.crawler.worker.story;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

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
	
	@Autowired
	private ApplicationContext applicationContext;
	
	@Value("${worker.initialDelay}")
	private int initialDelay;
	@Value("${worker.period}")
	private int period;
	@Value("${worker.story.jsonFileName}")
	private String storyJsonFile;
	
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
		
		// 按照顺序执行任务。当前是单线程调度任务。
		for (String url: story.getArgs()) {
			for (Map<String, String> task : story.getTasks()) {
				
				logger.info("Start task={}", task);
				if (task.get(Crawlers.TASK_PARAMS).equals(Crawlers.TASK_PARAMS_ARGS)) {
					// 从任务中获取参数，并提交
					logger.info("Submit task={} using template={}", url, task.get(Crawlers.TASK_TEMPLATE));
					taskWorker.submitTask(story.getName(), url, task.get(Crawlers.TASK_TEMPLATE));
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
		
	}
}
