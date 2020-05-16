package github.totyumengr.crawler.planner.impl;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
public class JssPlanner {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Value("${planner.jss.ak}")
	private String accessKey;
	@Value("${planner.jss.sk}")
	private String secretKey;
	@Value("${planner.jss.source}")
	private String source;
	
	@Value("${planner.initialDelay}")
	protected int initialDelay;
	@Value("${planner.period}")
	protected int period;
	@Value("${planner.step}")
	protected int step;
	
	@Autowired
	protected RedissonClient storyDataClient;

	private JingdongStorageService jss;
	
	private Map<String, String[]> seedMap = new HashMap<String, String[]>();
	
	@PostConstruct
	public void init() throws Exception {
		
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
					for(ObjectSummary objectSummary : objectListing.getObjectSummaries()){
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
					   
					   // 启动一个Plan
					   SavePointPlanner planner = new SavePointPlanner(storyDataClient, initialDelay, period, Executors.newSingleThreadScheduledExecutor()) {
							@Override
							protected ImmutablePair<Story, String> generateStory(String planName, String template, String savePoint) {
								return JssPlanner.this.generateStory(planName, seedTemplateContent, savePoint);
							}
						};
						// 执行
						planner.planExecute(objectSummary.getKey(), seedTemplateContent);
					}
				} catch (Exception e) {
					logger.info("Error jss watching...", e);
				}
			}
		}, initialDelay, period, TimeUnit.MINUTES);
	}
	
	protected ImmutablePair<Story, String> generateStory(String planName, String template, String savePoint) {
		
		Integer start = Integer.valueOf(savePoint == null ? "0" : savePoint);
		Integer end = start + step;
		
		String[] seedIds = seedMap.get(planName);
		Story story = Crawlers.GSON.fromJson(template, Story.class);
		story.setName(story.getName() + "-" + start + ".");
		String urlTemplate = story.getArgs().get(0);
		story.setArgs(new ArrayList<String>());
		boolean overflow = false;
		Integer lasted;
		if (end < seedIds.length) {
			lasted = end;
		} else {
			lasted = seedIds.length;
		}
		if (start >= seedIds.length) {
			overflow = true;
		}
		
		if (!overflow) {
			for (int i = start; i < lasted; i++) {
				String format = seedIds[i];
				try {
					format = URLEncoder.encode(seedIds[i], "UTF-8");
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
