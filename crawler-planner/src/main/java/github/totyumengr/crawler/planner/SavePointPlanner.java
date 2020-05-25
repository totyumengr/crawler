package github.totyumengr.crawler.planner;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.redisson.api.RListMultimap;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.StringUtils;

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
	@Value("${planner.recyclebin.maxtry}")
	protected int recycleBinMaxTry;
	
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
			return new ImmutablePair<Crawlers.Story, Integer>(story, lasted);
		} else {
			logger.info("Overflow... return null... {}", start);
			return new ImmutablePair<Crawlers.Story, Integer>(null, start);
		}
	}
	
	private static final String RECYCLE_BIN_STORY = "recyclebin.clean.";
	
	public void init() throws Exception {
		
		Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new Runnable() {
			
			@Override
			public void run() {
				try {
					List<String> running = getRunningPlan();
					for (String planName : running) {
						Object doneStory = storyDataClient.getQueue(planName + Crawlers.STORY_FILE_QUEYE_DONE).poll();
						if (doneStory != null) {
							logger.info("Found a story have done. {}", doneStory);
							Story story = Crawlers.GSON.fromJson(doneStory.toString(), Story.class);
							// 从Running列表中删除
							storyDataClient.getListMultimap(Crawlers.PLAN_STORY_RUNNING).get(planName).remove(story.getName());
							
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
								if (StringUtils.countOccurrencesOf(story.getName(), RECYCLE_BIN_STORY) < recycleBinMaxTry) {
									// 提交Story
									submitStory(planName, story);
								} else {
									logger.info("Give up recycle-bin story: {}", originalStoryName);
								}
								// 倾倒垃圾箱
								storyDataClient.getListMultimap(Crawlers.RECYCLE_BIN).fastRemove(originalStoryName);
								logger.info("Clean {} out recycle-bin.", originalStoryName);
							}
						}
						boolean isDone = planHasDone(planName);
						if (isDone) {
							// 清除Plan
							storyDataClient.getList(Crawlers.PLAN_RANNING).remove(planName);
							logger.info("Plan {} has done", planName);
							// 回调
							handlePlanDone(planName);
						}
					}
				} catch (Exception e) {
					logger.info("Error when try to clean recycle-bin", e);
				}
			}
		}, initialDelay, period, TimeUnit.SECONDS);
	}
	
	protected abstract void handlePlanDone(String planName);
	
	private void submitStory(String plannerName, Story story) {
		String storyJson = Crawlers.GSON.toJson(story);
		// 提交到工作队列
		storyDataClient.getQueue(Crawlers.STORY_FILE_QUEYE).add(storyJson);
		// 记录Story Running
		storyDataClient.getListMultimap(Crawlers.PLAN_STORY_RUNNING).get(plannerName).add(story.getName());
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
	
	public List<String> getRunningPlan() {
		
		List<String> running = storyDataClient.getList(Crawlers.PLAN_RANNING);
		return running == null ? new ArrayList<String>(0) : running;
	}
	
	public boolean planHasDone(String planName) {
		
		// 如果不在Running列表中
		if (!getRunningPlan().contains(planName)) {
			return true;
		}
		// 如果History是空的
		List<String> storyHistory = getStorysOfPlan(planName);
		if (storyHistory != null && storyHistory.size() == 0) {
			return true;
		}
		
		// 如果没有Doing状态的Story，并且Queue中是空的
		List<String> workerQueue = storyDataClient.getList(Crawlers.STORY_FILE_QUEYE);
		if (getRunningStorysOfPlan(planName).size() == 0 && workerQueue != null && workerQueue.size() == 0) {
			return true;
		}
		
		return false;
	}
	
	public List<String> getStorysOfPlan(String planName) {
		
		List<Object> storys = storyDataClient.getListMultimap(Crawlers.PLAN_STORY_RUNNING).get(planName);
		return storys == null ? new ArrayList<String>(0) : storys.stream().map(o -> o.toString()).collect(Collectors.toList());
	}
	
	public List<String> getRunningStorysOfPlan(String planName) {
		
		if (!StringUtils.hasLength(planName)) {
			return new ArrayList<String>(0);
		}
		
		RListMultimap<String, String> storys = storyDataClient.getListMultimap(Crawlers.STORY_FILE_QUEYE_DOING);
		if (storys == null) {
			return new ArrayList<String>(0);
		}
		
		List<String> running = new ArrayList<String>();
		for (String key : storys.readAllKeySet()) {
			for (String storyJson : storys.get(key)) {
				Story story = Crawlers.GSON.fromJson(storyJson, Story.class);
				if (planName.equals(story.getPlanName())) {
					running.add(story.getName());
				}
			}
		}
		return running;
	}
	
	// Key is task URL，值是这个Task的执行路径，Pair的Left是执行URL，Right是状态
	public Map<String, List<Pair<String, String>>> getTasksOfStory(String storyName) {
		
		RListMultimap<String, String> trace = storyDataClient.getListMultimap(storyName + Crawlers.STORY_TRACE);
		if (trace == null) {
			return new HashMap<String, List<Pair<String, String>>>(0);
		}
		
		Map<String, List<Pair<String, String>>> running = new LinkedHashMap<String, List<Pair<String, String>>>();
		for (String key : trace.readAllKeySet()) {
			List<String> taskTrace = trace.get(key);
			List<Pair<String, String>> traceStatus = new ArrayList<Pair<String, String>>();
			for (String task : taskTrace) {
				Task t = Crawlers.GSON.fromJson(task, Task.class);
				traceStatus.add(new ImmutablePair<String, String>(t.getFromUrl(), t.getStatus()));
			}
			running.put(key, traceStatus);
		}
		return running;
	}
}
