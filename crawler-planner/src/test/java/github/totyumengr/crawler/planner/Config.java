package github.totyumengr.crawler.planner;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import github.totyumengr.crawler.planner.impl.FromFilePlanner;

@TestConfiguration
public class Config {
	
	@Bean
	public FromFilePlanner fromFilePlanner() {
		
		FromFilePlanner planner = new FromFilePlanner();
		planner.setStoryTempalteName("book-douban-byId.json");
		planner.setFileName("book_id.1");
		
		return planner;
	}
}