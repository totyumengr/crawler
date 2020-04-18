package github.totyumengr.crawler.extractor.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.redisson.api.RedissonClient;
import org.seimicrawler.xpath.JXDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import github.totyumengr.crawler.Crawlers;
import github.totyumengr.crawler.extractor.Extractor;

public abstract class AbstractExtractor implements Extractor {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Autowired
	protected RedissonClient extractDataClient;
	
	protected abstract Map<String, Object> doExtract(String url, JXDocument document, List<List<String>> coreData);

	@Override
	public boolean extract(String url, JXDocument document, String extractor) {
		
		try {
			Map<String, Object> structData = new HashMap<String, Object>();
			List<List<String>> coreData = new ArrayList<List<String>>();
			structData.put(Crawlers.EXTRACT_DATA, coreData);
			
			// 执行模板方法
			Map<String, Object> extraData = doExtract(url, document, coreData);
			if (extraData != null) {
				structData.putAll(extraData);
			}
			
    		// 解析完成，转换为JSON进行存储
    		String json = Crawlers.GSON.toJson(structData);
    		extractDataClient.getMap(Crawlers.PREFIX_EXTRACT_DATA + extractor).put(url, json);
    		logger.info("Success to extract for url={}, push into {}", url, Crawlers.PREFIX_EXTRACT_DATA);
        } catch (Exception e) {
            logger.error("Can not extract any result.", e);
            return false;
        }
		
		return true;
	}

}
