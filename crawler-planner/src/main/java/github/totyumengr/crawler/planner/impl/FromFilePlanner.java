package github.totyumengr.crawler.planner.impl;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import github.totyumengr.crawler.Crawlers;
import github.totyumengr.crawler.Crawlers.Story;
import github.totyumengr.crawler.planner.SavePointPlanner;

/**
 * 给定ID集合的执行计划
 * @author mengran7
 *
 */
@Component
@ConditionalOnProperty(value="planner.local", havingValue = "true")
public class FromFilePlanner {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Value("${planner.initialDelay}")
	protected int initialDelay;
	@Value("${planner.period}")
	protected int period;
	@Value("${planner.step}")
	protected int step;
	
	@Value("${planner.storytpl.dir}")
	private String storyDir;
	
	@Autowired
	protected RedissonClient storyDataClient;
	
	private String[] bookIds;
	private String storyTempalteName;
	private String fileName;
	
	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getStoryTempalteName() {
		return storyTempalteName;
	}

	public void setStoryTempalteName(String storyTempalteName) {
		this.storyTempalteName = storyTempalteName;
	}

	@PostConstruct
	public void init() throws Exception {
		
		this.setFileName("book_id.1");
		this.setStoryTempalteName("book-douban-byId.json");
		
		File f = new File (storyDir, getFileName());
		String doubanTemplate = FileUtils.readFileToString(f, "UTF-8");
		bookIds = doubanTemplate.split("\r\n");
		if (bookIds.length < 2) {
			bookIds = doubanTemplate.split("\n");
		}
		
		f = new File (storyDir, getStoryTempalteName());
		final String storyTemplate = FileUtils.readFileToString(f, "UTF-8");
		
		// 启动一个Plan
	   SavePointPlanner planner = new SavePointPlanner(storyDataClient, initialDelay, period, Executors.newSingleThreadScheduledExecutor()) {
			@Override
			protected ImmutablePair<Story, String> generateStory(String planName, String template, String savePoint) {
				return FromFilePlanner.this.generateStory(planName, storyTemplate, savePoint);
			}
		};
		// 执行
		planner.planExecute(getFileName(), doubanTemplate);
	}
	
	protected ImmutablePair<Story, String> generateStory(String planName, String template, String savePoint) {
		
		Integer start = Integer.valueOf(savePoint == null ? "0" : savePoint);
		Integer end = start + step;
		
		Story story = Crawlers.GSON.fromJson(template, Story.class);
		story.setName(story.getName() + "-" + start + ".");
		String urlTemplate = story.getArgs().get(0);
		story.setArgs(new ArrayList<String>());
		boolean overflow = false;
		Integer lasted;
		if (end < bookIds.length) {
			lasted = end;
		} else {
			lasted = bookIds.length;
		}
		if (start >= bookIds.length) {
			overflow = true;
		}
		
		if (!overflow) {
			for (int i = start; i < lasted; i++) {
				String format = bookIds[i];
				try {
					format = URLEncoder.encode(bookIds[i], "UTF-8");
				} catch (UnsupportedEncodingException e) {
					// Do nothing
				}
				story.getArgs().add(String.format(urlTemplate, format));
			}
			logger.info("Fill {} into story..", story.getArgs().size());
			return new ImmutablePair<Crawlers.Story, String>(story, end.toString());
		} else {
			logger.info("Overflow... return null... {}", savePoint);
			return new ImmutablePair<Crawlers.Story, String>(null, end.toString());
		}
	}
}
