package github.totyumengr.crawler.extractor.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.redisson.api.RedissonClient;
import org.seimicrawler.xpath.JXDocument;
import org.seimicrawler.xpath.JXNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import github.totyumengr.crawler.Crawlers;
import github.totyumengr.crawler.extractor.Extractor;

@Component("content")
public class ContentExtractor implements Extractor {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Autowired
	private RedissonClient contentClient;

	@Override
	public boolean extract(String url, JXDocument document, Map<String, Object> structData) {
		
		try {
			
			// 开始解析结果列表
            List<String> elements = extractContent(url, document);
            structData.put(Crawlers.XPATH_CONTENT, elements);
    		
    		// 解析完成，转换为JSON进行存储
    		String json = Crawlers.GSON.toJson(structData);
    		contentClient.getMap(Crawlers.EXTRACT_DATA_PREFIX + structData.get(Crawlers.EXTACT_TYPE)).put(url, json);
    		logger.info("Success to extract for url={}, push into {}", url, Crawlers.EXTRACT_DATA_PREFIX);
        } catch (Exception e) {
            logger.error("Can not extract any result.", e);
            return false;
        }
		
		return true;
	}
	
	/**
	 * 
	 * @param url 抓取URL
	 * @param document 页面文档
	 * @return 结构化提取的内容
	 */
	protected List<String> extractContent(String url, JXDocument document) {
		
		// 根据配置规则进行元素级内容的提取，并且进行结构化存储。
		Object blockXpath = contentClient.getMap(Crawlers.XPATH_CONTENT).get(url);
		if (blockXpath == null) {
			logger.info("{} Return because can not found {} of url={}", Crawlers.PLEASE_SET_EXTRACT_XPATH, Crawlers.XPATH_CONTENT, url);
			List<String> html = new ArrayList<String>(1);
			html.add(Crawlers.PLEASE_SET_EXTRACT_XPATH);
			return html;
		}
		
		List<String> struct = new ArrayList<String>();
		String[] xpaths = blockXpath.toString().split("\\|");
		for (String xpath : xpaths) {
			// By 元素提取
			List<JXNode> nodes = document.selN(xpath);
			if (nodes != null) {
				for (JXNode node : nodes) {
					struct.add(node.asString());		
				}
			}
		}
		logger.info("Content={} return of url={}", struct, url);
		
		return struct;
	}
}
