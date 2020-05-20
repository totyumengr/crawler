package github.totyumengr.crawler.extractor.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.seimicrawler.xpath.JXDocument;
import org.seimicrawler.xpath.JXNode;
import org.springframework.stereotype.Component;

import github.totyumengr.crawler.Crawlers;
import github.totyumengr.crawler.extractor.Extractor;

@Component("content")
public class ContentExtractor extends AbstractExtractor implements Extractor {
	
	@Override
	protected Map<String, Object> doExtract(String storyName, String url, String html, List<List<String>> coreData, String status, String ip) {
		
		JXDocument document = JXDocument.create(html);
		// 开始解析结果列表
        List<String> elements = extractContent(storyName, url, document, html, status, ip);
        coreData.add(elements);
        
        // 没有额外的解析数据
        return null;
	}
	
	/**
	 * 
	 * @param url 抓取URL
	 * @param document 页面文档
	 * @return 结构化提取的内容
	 */
	protected List<String> extractContent(String storyName, String url, JXDocument document, String html, String status, String ip) {
		
		// 根据配置规则进行元素级内容的提取，并且进行结构化存储。
		Object blockXpath = extractDataClient.getMap(storyName + Crawlers.XPATH_CONTENT).get(url);
		if (blockXpath == null) {
			logger.info("{} Return because can not found {} of url={}", Crawlers.PLEASE_SET_EXTRACT_XPATH, Crawlers.XPATH_CONTENT, url);
			List<String> forReturn = new ArrayList<String>(1);
			forReturn.add(Crawlers.PLEASE_SET_EXTRACT_XPATH);
			return forReturn;
		}
		
		List<String> struct = new ArrayList<String>();
		String[] xpaths = blockXpath.toString().split("\\|");
		logger.info("Extract data using xpaths {}", blockXpath.toString());
		for (String xpath : xpaths) {
			// By 元素提取
			List<JXNode> nodes = document.selN(xpath);
			logger.info("Extract data using xpath {} found {}", xpath, nodes == null ? 0 : nodes.size());
			if (nodes != null) {
				for (JXNode node : nodes) {
					struct.add(node.toString());
				}
			}
		}
		logger.info("Contents found={} return of url={}", struct.size(), url);
		
		// 没有解析出数据的情况
		if (struct.size() == 0) {
			logger.info("Can not extract data from {}", html);
			Object antiXpath = extractDataClient.getMap(storyName + Crawlers.XPATH_CONTENT_ANTI).get(url);
			if (antiXpath != null) {
				logger.info("Anti check using {}", antiXpath.toString());
				String[] antiXpaths = antiXpath.toString().split("\\|");
				for (String xpath : antiXpaths) {
					JXNode node = document.selNOne(xpath);
					if (node != null || html.contains(xpath)) {
						extractDataClient.getMap(storyName + Crawlers.EXTRACTOR_CONTENT_ANTI_ALERT).put(url, html);
						if (ip != null) {
							extractDataClient.getQueue(Crawlers.PROXYIP_ANTI).add(ip);
						}
						logger.info("ALERT: {}, {} by {}", url, html, antiXpath);
						break;
					}
				}
			}
		}
		
		return struct;
	}

}
