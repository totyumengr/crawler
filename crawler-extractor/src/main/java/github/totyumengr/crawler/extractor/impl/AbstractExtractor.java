package github.totyumengr.crawler.extractor.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import github.totyumengr.crawler.Crawlers;
import github.totyumengr.crawler.extractor.Extractor;

public abstract class AbstractExtractor implements Extractor {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Autowired
	protected RedissonClient extractDataClient;
	
	@Value("${extractor.structdata.ttl}")
	private int ttl;
	
	protected abstract Map<String, Object> doExtract(String storyName, String url, String html, List<List<String>> coreData, String status, String ip);

	@Override
	public boolean extract(String storyName, String url, String html, String extractor, String status, String ip) {
		
		Map<String, Object> structData = new HashMap<String, Object>();
		try {
			List<List<String>> coreData = new ArrayList<List<String>>();
			structData.put(Crawlers.EXTRACT_DATA, coreData);
			
			// 执行模板方法
			Map<String, Object> extraData = doExtract(storyName, url, html, coreData, status, ip);
			if (extraData != null) {
				structData.putAll(extraData);
			}
        } catch (Exception e) {
            logger.error("Can not extract any result.", e);
            return false;
        } finally {
        	// 解析完成，转换为JSON进行存储
    		String json = Crawlers.GSON.toJson(structData);
    		extractDataClient.getQueue(storyName + Crawlers.EXTRACT_STRUCT_DATA + extractor + url).add(json);
    		extractDataClient.getQueue(storyName + Crawlers.EXTRACT_STRUCT_DATA + extractor + url).expire(ttl, TimeUnit.DAYS);
    		logger.info("Success to extract for url={}, push into {}", url, Crawlers.EXTRACT_STRUCT_DATA);
        }
		
		return true;
	}

}
