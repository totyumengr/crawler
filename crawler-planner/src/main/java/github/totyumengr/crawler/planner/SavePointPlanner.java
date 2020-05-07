package github.totyumengr.crawler.planner;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import github.totyumengr.crawler.Crawlers;
import github.totyumengr.crawler.Crawlers.Story;

/**
 * 支持断点续传的任务执行器
 * @author mengran7
 *
 */
public abstract class SavePointPlanner {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Autowired
	private RedissonClient storyDataClient;
	
	@Value("${planner.initialDelay}")
	private int initialDelay;
	@Value("${planner.period}")
	private int period;
	
	@Value("${planner.step}")
	protected int step;
	@Value("${planner.storytpl.dir}")
	protected String storyDir;
	
	public static final String PLANNER_SAVEPOINT = "planner.savepoint";
	public static final String STORY_QUEUE = "worker.story";
	
	protected abstract String templateName();
	
	protected abstract ImmutablePair<Story, String> generateStory(String template, String savePoint);
	
	public final CountDownLatch END = new CountDownLatch(1);
	
	@PostConstruct
	private void init() throws Exception {
		
		ScheduledExecutorService storyScanner = Executors.newSingleThreadScheduledExecutor();
		
		File f = new File (storyDir, templateName());
		String doubanTemplate = FileUtils.readFileToString(f, "UTF-8");
		
		logger.info("Start to plan {}", doubanTemplate);
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
					
					storyInQueue = storyDataClient.getMap(PLANNER_SAVEPOINT).get(templateName());
					ImmutablePair<Story, String> forReturn = generateStory(doubanTemplate, storyInQueue == null ? null : storyInQueue.toString());
					if (forReturn.getLeft() == null) {
						// Means is done...
						storyScanner.shutdown();
						END.countDown();
						logger.info("Shutdown...");
					} else {
						String storyJson = Crawlers.GSON.toJson(forReturn.getLeft());
						storyDataClient.getQueue(STORY_QUEUE).add(storyJson);
						
						storyDataClient.getMap(PLANNER_SAVEPOINT).put(templateName(), forReturn.getRight());
						// 找到一个Story
						logger.info("Planing a story and submit it. {}", storyJson);
					}
				} catch (Exception e) {
					logger.error("Error when try to plan.", e);
				}
			}
		}, initialDelay, period, TimeUnit.SECONDS);
	}
}
