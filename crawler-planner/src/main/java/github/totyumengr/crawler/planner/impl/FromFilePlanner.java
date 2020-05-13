package github.totyumengr.crawler.planner.impl;

import java.io.File;
import java.util.ArrayList;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import github.totyumengr.crawler.Crawlers;
import github.totyumengr.crawler.Crawlers.Story;
import github.totyumengr.crawler.planner.SavePointPlanner;

/**
 * 给定ID集合的执行计划
 * @author mengran7
 *
 */
public class FromFilePlanner extends SavePointPlanner {
	
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
		
		File f = new File (storyDir, getFileName());
		String doubanTemplate = FileUtils.readFileToString(f, "UTF-8");
		bookIds = doubanTemplate.split("\n");
	}
	
	@Override
	protected ImmutablePair<Story, String> generateStory(String template, String savePoint) {
		
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
				story.getArgs().add(String.format(urlTemplate, bookIds[i]));
			}
			logger.info("Fill {} into story..", story.getArgs().size());
			return new ImmutablePair<Crawlers.Story, String>(story, end.toString());
		} else {
			logger.info("Overflow... return null... {}", savePoint);
			return null;
		}
	}

	@Override
	protected String templateName() {
		return getStoryTempalteName();
	}
}
