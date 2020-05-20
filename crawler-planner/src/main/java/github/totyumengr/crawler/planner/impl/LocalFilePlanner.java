package github.totyumengr.crawler.planner.impl;

import java.io.File;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class LocalFilePlanner extends SavePointPlanner {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Value("${planner.step}")
	protected int step;
	@Value("${planner.storytpl.dir}")
	private String storyDir;
	
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
		
		// 执行
		this.planExecute(getFileName(), storyTemplate);
	}
	
	protected ImmutablePair<Story, Integer> generateStory(String planName, String template, int savePoint) {
		
		Integer start = savePoint;
		
		Story story = Crawlers.GSON.fromJson(template, Story.class);
		story.setName(story.getName() + "-" + start + ".");
		story.setPlanName(planName);
		String urlTemplate = story.getArgs().get(0);
		
		return fillStory(story, start, urlTemplate, bookIds, step);
	}
}
