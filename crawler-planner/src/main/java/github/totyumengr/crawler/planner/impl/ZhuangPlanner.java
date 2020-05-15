package github.totyumengr.crawler.planner.impl;

import org.apache.commons.lang3.tuple.ImmutablePair;

import github.totyumengr.crawler.Crawlers;
import github.totyumengr.crawler.Crawlers.Story;
import github.totyumengr.crawler.planner.SavePointPlanner;

/**
 * 撞ID相关的执行计划
 * @author mengran7
 *
 */
public class ZhuangPlanner extends SavePointPlanner {
	
	private String storyTempalteName;
	
	public String getStoryTempalteName() {
		return storyTempalteName;
	}

	public void setStoryTempalteName(String storyTempalteName) {
		this.storyTempalteName = storyTempalteName;
	}
	
	@Override
	protected String templateName() {
		return getStoryTempalteName();
	}

	@Override
	protected String plannerName() {
		return "zhuang";
	}

	@Override
	protected ImmutablePair<Story, String> generateStory(String template, String savePoint) {
		
		Integer start = Integer.valueOf(savePoint == null ? "0" : savePoint);
		Integer end = start + step;
		String argsEL = start + "," + end;
		
		Story story = Crawlers.GSON.fromJson(template, Story.class);
		story.setName(story.getName() + "-" + start + ".");
		story.setArgsEL(argsEL);
		
		// TODO: 需要设置退出条件，比如最大的ID是多少
		return new ImmutablePair<Crawlers.Story, String>(story, end.toString());
	}
}
