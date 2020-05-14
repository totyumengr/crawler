package github.totyumengr.crawler.planner;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import github.totyumengr.crawler.planner.impl.FromFilePlanner;

@TestConfiguration
public class SmzdmKwsConfig {
	
	@Bean
	public FromFilePlanner fromFilePlanner() {
		
		FromFilePlanner planner = new FromFilePlanner();
		planner.setStoryTempalteName("smzdm-post-byKw.json");
		planner.setFileName("6_1.csv");
		
		return planner;
	}
}
