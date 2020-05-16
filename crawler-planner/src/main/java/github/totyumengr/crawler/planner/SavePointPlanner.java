package github.totyumengr.crawler.planner;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import github.totyumengr.crawler.Crawlers;
import github.totyumengr.crawler.Crawlers.Story;

/**
 * 支持断点续传的任务执行器
 * @author mengran7
 *
 */
public abstract class SavePointPlanner {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	public static final String PLANNER_SAVEPOINT = "planner.savepoint";
	public static final String PLANNER_STORY_HISTORY = "planner.story";
	public static final String STORY_QUEUE = "worker.story";
	
	protected RedissonClient storyDataClient;
	protected int initialDelay;
	protected int period;
	protected ScheduledExecutorService storyScanner;
	
	public SavePointPlanner(RedissonClient storyDataClient, int initialDelay, int period,
			ScheduledExecutorService storyScanner) {
		super();
		this.storyDataClient = storyDataClient;
		this.initialDelay = initialDelay;
		this.period = period;
		this.storyScanner = storyScanner;
	}

	public ScheduledExecutorService getStoryScanner() {
		return storyScanner;
	}

	public void setStoryScanner(ScheduledExecutorService storyScanner) {
		this.storyScanner = storyScanner;
	}

	public RedissonClient getStoryDataClient() {
		return storyDataClient;
	}

	public void setStoryDataClient(RedissonClient storyDataClient) {
		this.storyDataClient = storyDataClient;
	}
	
	public int getInitialDelay() {
		return initialDelay;
	}

	public void setInitialDelay(int initialDelay) {
		this.initialDelay = initialDelay;
	}

	public int getPeriod() {
		return period;
	}

	public void setPeriod(int period) {
		this.period = period;
	}

	protected abstract ImmutablePair<Story, String> generateStory(String planName, String template, String savePoint);
	
	public final CountDownLatch END = new CountDownLatch(1);
	
	protected void handlerPlannerClose() {
		// Do nothing
	}
	
	/**
	 * 开始执行一个Plan，按照模板生成Story，并提交
	 * @param plannerName
	 * @param storyTemplate
	 * @throws Exception
	 */
	public void planExecute(String plannerName, String storyTemplate) throws Exception {
		
		logger.info("Start to plan {}", plannerName);
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
					
					storyInQueue = storyDataClient.getMap(PLANNER_SAVEPOINT).get(plannerName);
					ImmutablePair<Story, String> forReturn = generateStory(plannerName, storyTemplate, storyInQueue == null ? null : storyInQueue.toString());
					if (forReturn.getLeft() == null) {
						// Means is done...
						handlerPlannerClose();
						
						storyDataClient.getMap(PLANNER_SAVEPOINT).put(plannerName, 0);
						storyScanner.shutdown();
						END.countDown();
						logger.info("Shutdown...");
					} else {
						String storyJson = Crawlers.GSON.toJson(forReturn.getLeft());
						// 提交到工作队列
						storyDataClient.getQueue(STORY_QUEUE).add(storyJson);
						
						// 设置Save Point
						storyDataClient.getMap(PLANNER_SAVEPOINT).put(plannerName, forReturn.getRight());
						// 存入History
						storyDataClient.getListMultimap(PLANNER_STORY_HISTORY).get(plannerName).add(storyJson);
						
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
