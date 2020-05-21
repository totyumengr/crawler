package github.totyumengr.crawler.planner.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.jd.jss.Credential;
import com.jd.jss.JingdongStorageService;
import com.jd.jss.domain.ObjectListing;
import com.jd.jss.domain.ObjectSummary;
import com.jd.jss.domain.StorageObject;

import github.totyumengr.crawler.Crawlers;
import github.totyumengr.crawler.Crawlers.Story;
import github.totyumengr.crawler.planner.SavePointPlanner;

/**
 * Base on JSS
 * @author mengran7
 *
 */
@Component
@ConditionalOnProperty(value="planner.local", havingValue = "false", matchIfMissing = true)
public class JssPlanner extends SavePointPlanner {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Value("${planner.jss.ak}")
	private String accessKey;
	@Value("${planner.jss.sk}")
	private String secretKey;
	@Value("${planner.jss.source}")
	private String source;
	
	@Value("${planner.step}")
	protected int step;

	private JingdongStorageService jss;
	
	private Map<String, String[]> seedMap = new HashMap<String, String[]>();
	private Set<String> plannerStatus = new HashSet<String>();
	
	@PostConstruct
	public void init() throws Exception {
		// 启动回捞
		super.init();
		
		jss = new JingdongStorageService(new Credential(accessKey, secretKey));
		boolean have = jss.hasBucket(source);
		if (!have) {
			throw new Exception("Can not found bucket: " + source);
		}
		logger.info("Connected jss with {} and start watching {}", source);
		
		// 开始扫描Source目录
		Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new Runnable() {
			
			@Override
			public void run() {
				try {
					ObjectListing objectListing = jss.bucket(source).prefix("seed/").listObject();
					for(ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
						if (plannerStatus.contains(objectSummary.getKey())) {
							// 跳过
							continue;
						}
						logger.info("Found crawler seed {}", objectSummary.getKey());
						StorageObject storage = jss.bucket(source).object(objectSummary.getKey()).get();
						String seedContent = IOUtils.toString(storage.getInputStream(), "UTF-8");
						String[] seeds = seedContent.split("\r\n");
						if (seeds.length < 2) {
							seeds = seedContent.split("\n");
						}
						logger.info("Found {} crawler seed {}", seeds.length, objectSummary.getKey());
						// TODO: 太占内存了。以后想个办法。
						seedMap.put(objectSummary.getKey(), seeds);
						
						StorageObject seedTemplate = jss.bucket(source).prefix("template/").object(objectSummary.getKey()).get();
						String seedTemplateContent = IOUtils.toString(seedTemplate.getInputStream(), "UTF-8");
						// 执行
						JssPlanner.this.planExecute(objectSummary.getKey(), seedTemplateContent);
						plannerStatus.add(objectSummary.getKey());
					}
				} catch (Exception e) {
					logger.info("Error jss watching...", e);
				}
			}
		}, initialDelay, period, TimeUnit.MINUTES);
	}
	
	protected ImmutablePair<Story, Integer> generateStory(String planName, String template, int savePoint) {
		
		Integer start = savePoint;
		String[] seedIds = seedMap.get(planName);
		
		Story story = Crawlers.GSON.fromJson(template, Story.class);
		story.setName(story.getName() + "-" + start + ".");
		story.setPlanName(planName);
		String urlTemplate = story.getArgs().get(0);
		
		return fillStory(story, start, urlTemplate, seedIds, step);
	}
}
