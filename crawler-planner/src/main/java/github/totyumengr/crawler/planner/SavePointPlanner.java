package github.totyumengr.crawler.planner;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import github.totyumengr.crawler.Crawlers;
import github.totyumengr.crawler.Crawlers.Story;
import github.totyumengr.crawler.Crawlers.Task;

/**
 * 支持断点续传的任务执行器
 * @author mengran7
 *
 */
public abstract class SavePointPlanner {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Autowired
	protected RedissonClient storyDataClient;
	@Value("${planner.initialDelay}")
	protected int initialDelay;
	@Value("${planner.period}")
	protected int period;
	
	public SavePointPlanner() {
		super();
	}

	protected abstract ImmutablePair<Story, Integer> generateStory(String planName, String template, int savePoint);
	
	protected final ImmutablePair<Story, Integer> fillStory(Story story, Integer start, String urlTemplate, String[] ids, int step) {
		
		story.setArgs(new ArrayList<String>());
		Integer end = start + step;
		
		boolean overflow = false;
		Integer lasted;
		if (end < ids.length) {
			lasted = end;
		} else {
			lasted = ids.length;
		}
		if (start >= ids.length) {
			overflow = true;
		}
		
		if (!overflow) {
			for (int i = start; i < lasted; i++) {
				String format = ids[i];
				try {
					format = URLEncoder.encode(ids[i], "UTF-8");
				} catch (UnsupportedEncodingException e) {
					// Do nothing
				}
				story.getArgs().add(String.format(urlTemplate, format));
			}
			logger.info("Fill {} into story..", story.getArgs().size());
			return new ImmutablePair<Crawlers.Story, Integer>(story, end);
		} else {
			logger.info("Overflow... return null... {}", start);
			return new ImmutablePair<Crawlers.Story, Integer>(null, end);
		}
	}
	
	private static final String RECYCLE_BIN_STORY = "recyclebin.clean.";
	
	public void init() throws Exception {
		
		Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new Runnable() {
			
			@Override
			public void run() {
				try {
					List<Object> running = storyDataClient.getList(Crawlers.PLAN_RANNING);
					for (Object r : running) {
						String planName = r.toString();
						Object doneStory = storyDataClient.getQueue(planName + Crawlers.STORY_FILE_QUEYE_DONE).poll();
						if (doneStory != null) {
							logger.info("Found a story have done. {}", doneStory);
							Story story = Crawlers.GSON.fromJson(doneStory.toString(), Story.class);
							// 从Running列表中删除
							storyDataClient.getSetMultimap(Crawlers.PLAN_STORY_RUNNING).get(planName).remove(story.getName());
							
							// 找到这个Story相关的recycle-bin
							List<Object> recycleBin = storyDataClient.getListMultimap(Crawlers.RECYCLE_BIN).get(story.getName());
							if (recycleBin != null && !recycleBin.isEmpty()) {
								List<String> taskUrls = new ArrayList<String>(recycleBin.size());
								for (Object bin : recycleBin) {
									Task task = Crawlers.GSON.fromJson(bin.toString(), Task.class);
									taskUrls.add(task.getFromUrl());
								}
								// 重新提交垃圾箱中的Task
								String originalStoryName = story.getName();
								story.setName(originalStoryName + RECYCLE_BIN_STORY);
								story.setArgs(taskUrls);
								// 提交Story
								submitStory(planName, story);
								// 倾倒垃圾箱
								storyDataClient.getListMultimap(Crawlers.RECYCLE_BIN).fastRemove(originalStoryName);
								logger.info("Clean {} out recycle-bin.", originalStoryName);
							}
						}
						int runningStory = storyDataClient.getSetMultimap(Crawlers.PLAN_STORY_RUNNING).get(planName).size();
						logger.info("Plan {} has running story {}", planName, runningStory);
						if (runningStory == 0) {
							// 清除Plan
							storyDataClient.getList(Crawlers.PLAN_RANNING).remove(planName);
							logger.info("Plan {} has done", planName);
						}
					}
				} catch (Exception e) {
					logger.info("Error when try to clean recycle-bin", e);
				}
			}
		}, initialDelay, period, TimeUnit.SECONDS);
	}
	
	private void submitStory(String plannerName, Story story) {
		String storyJson = Crawlers.GSON.toJson(story);
		// 提交到工作队列
		storyDataClient.getQueue(Crawlers.STORY_FILE_QUEYE).add(storyJson);
		// 记录Story Running
		storyDataClient.getSetMultimap(Crawlers.PLAN_STORY_RUNNING).get(plannerName).add(story.getName());
		// 找到一个Story
		logger.info("Planing a story and submit it. {}", storyJson);
	}
	
	/**
	 * 开始执行一个Plan，按照模板生成Story，并提交
	 * @param plannerName
	 * @param storyTemplate
	 * @throws Exception
	 */
	public boolean planExecute(String plannerName, String storyTemplate) throws Exception {
		
		logger.info("Start to plan {}", plannerName);
		boolean isRunning = storyDataClient.getList(Crawlers.PLAN_RANNING).contains(plannerName);
		if (isRunning) {
			logger.info("Plan {} is running", plannerName);
			return false;
		}
		
		storyDataClient.getList(Crawlers.PLAN_RANNING).add(plannerName);
		ImmutablePair<Story, Integer> forReturn = null;
		
		Object sp = storyDataClient.getMap(Crawlers.PLAN_SAVE_POINT).get(plannerName);
		int savePoint = sp == null ? 0 : Integer.valueOf(sp.toString());
		
		do {
			forReturn = generateStory(plannerName, storyTemplate, savePoint);
			if (forReturn.getLeft() != null) {
				savePoint = forReturn.getRight();
				submitStory(plannerName, forReturn.getKey());
				storyDataClient.getMap(Crawlers.PLAN_SAVE_POINT).put(plannerName, savePoint);
			}
		} while (forReturn.getLeft() != null);
		logger.info("End to plan {}", plannerName);
		return true;
	}
}
