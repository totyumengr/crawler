package github.totyumengr.crawler.planner;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import github.totyumengr.crawler.Crawlers;
import github.totyumengr.crawler.Crawlers.Story;

/**
 * 撞ID相关的执行计划
 * @author mengran7
 *
 */
@Component
public class ZhuangPlanner {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Autowired
	private RedissonClient storyDataClient;
	
	@Value("${planner.zhuang.initialDelay}")
	private int initialDelay;
	@Value("${planner.zhuang.period}")
	private int period;
	
	@Value("${planner.zhuang.step}")
	private int step;
	@Value("${planner.storytpl.dir}")
	private String storyDir;
	
	public static final String ZHUANG_PLANNER_SAVEPOINT = "planner.zhuang.savepoint";
	public static final String STORY_QUEUE = "worker.story";
	public static final String ZHUANG_TEMPLATE = "book-douban-zhuang.json";
	
	@PostConstruct
	private void init() throws Exception {
		
		ScheduledExecutorService storyScanner = Executors.newSingleThreadScheduledExecutor();
		
		File f = new File (storyDir, ZHUANG_TEMPLATE);
		String doubanTemplate = FileUtils.readFileToString(f, "UTF-8");
		
		logger.info("Start to plan Zhuang");
		storyScanner.scheduleWithFixedDelay(new Runnable() {

			@Override
			public void run() {
				
				Object storyInQueue = null;
				try {
					Object workerQ = storyDataClient.getQueue(STORY_QUEUE).peek();
					if (workerQ != null) {
						// 如果当前Story队列还有任务，那就
						logger.info("Do nothing because story queue is not empty.");
						return;
					}
					
					storyInQueue = storyDataClient.getMap(ZHUANG_PLANNER_SAVEPOINT).get(ZHUANG_TEMPLATE);
					Integer start = Integer.valueOf(storyInQueue == null ? "0" : storyInQueue.toString());
					Integer end = start + step;
					String argsEL = start + "," + end;
					
					Story story = Crawlers.GSON.fromJson(doubanTemplate, Story.class);
					story.setName(story.getName() + "-" + start);
					story.setArgsEL(argsEL);
					
					String storyJson = Crawlers.GSON.toJson(story);
					storyDataClient.getQueue(STORY_QUEUE).add(storyJson);
					
					storyDataClient.getMap(ZHUANG_PLANNER_SAVEPOINT).put(ZHUANG_TEMPLATE, end);
					// 找到一个Story
					logger.info("Planing a story and submit it. {}", storyJson);
				} catch (Exception e) {
					logger.error("Error when try to plan.", e);
				}
			}
		}, initialDelay, period, TimeUnit.SECONDS);
	}
}
