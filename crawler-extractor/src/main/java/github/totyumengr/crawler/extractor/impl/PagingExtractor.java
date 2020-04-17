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

@Component("paging")
public class PagingExtractor implements Extractor {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Autowired
	private RedissonClient pagingClient;

	@Override
	public boolean extract(String url, JXDocument document, Map<String, Object> structData) {
		
		try {
			Object listXpath = pagingClient.getMap(Crawlers.XPATH_LIST_ELEMENTS).get(url);
			if (listXpath == null) {
				logger.info("Directly return because can not found {} of url={}", Crawlers.XPATH_LIST_ELEMENTS, url);
				return false;
			}
			
			// 开始解析结果列表
            List<JXNode> records = document.selN(listXpath.toString());
            if (records != null) {
	            logger.info("Using {} to extract block count={}", listXpath, records.size());
	            // 继续结构化提取每一个区块的内容
	            List<List<String>> recordStructData = new ArrayList<List<String>>();
	            for (JXNode record : records) {
	            	List<String> struct = extractRecord(url, record);
	            	recordStructData.add(struct);
	            }
	            structData.put(Crawlers.XPATH_LIST_ELEMENTS, recordStructData);
            }
            
            // 开始解析分页
            Object barXpath = pagingClient.getMap(Crawlers.XPATH_PAGINGBAR_ELEMENTS).get(url);
    		if (barXpath == null) {
    			logger.info("Can not extract pagingbar of url={}", url);
    		} else {
    			JXNode bar = document.selNOne(barXpath.toString());
    			String nextPageUrl = extractPagingBar(url, bar);
    			structData.put(Crawlers.XPATH_PAGINGBAR_NEXTURL_ELEMENTS, nextPageUrl);
    		}
    		
    		// 解析完成，转换为JSON进行存储
    		String json = Crawlers.GSON.toJson(structData);
    		pagingClient.getMap(Crawlers.EXTRACT_DATA_PREFIX + structData.get(Crawlers.EXTACT_TYPE)).put(url, json);
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
	 * @param blockNode 页面里面的某一条列表节点
	 * @return 结构化提取的内容
	 */
	protected List<String> extractRecord(String url, JXNode blockNode) {
		
		// 根据配置规则进行元素级内容的提取，并且进行结构化存储。
		Object blockXpath = pagingClient.getMap(Crawlers.XPATH_RECORD_ELEMENTS).get(url);
		if (blockXpath == null) {
			logger.info("{} Return because can not found {} of url={}", Crawlers.PLEASE_SET_EXTRACT_XPATH, Crawlers.XPATH_RECORD_ELEMENTS, url);
			List<String> html = new ArrayList<String>(1);
			html.add(Crawlers.PLEASE_SET_EXTRACT_XPATH);
			return html;
		}
		
		List<String> struct = new ArrayList<String>();
		String[] xpaths = blockXpath.toString().split("\\|");
		for (String xpath : xpaths) {
			// By 元素提取
			JXNode node = blockNode.selOne(xpath);
			if (node != null) {
				struct.add(node.asString());	
			}
		}
		logger.info("Record={} return of url={}", struct, url);
		
		return struct;
	}
	
	/**
	 * 
	 * @param url 抓取URL
	 * @param pagingNode 分页节点
	 * @return 下一页的URL
	 */
	protected String extractPagingBar(String url, JXNode pagingNode) {
		
		// 解析当前页标识，并且发起下一页的抓取请求，并且设置解析器类型
		Object nextUrl = pagingClient.getMap(Crawlers.XPATH_PAGINGBAR_NEXTURL_ELEMENTS).get(url);
		if (nextUrl == null) {
			logger.info("{} Return because can not found {} of url={}", Crawlers.PLEASE_SET_EXTRACT_XPATH, Crawlers.XPATH_PAGINGBAR_NEXTURL_ELEMENTS, url);
			return null;
		}
		JXNode node = pagingNode.selOne(nextUrl.toString());
		if (node == null) {
			logger.info("Maybe last page...", url);
			return null;
		}
		
		String nextPageUrl = node.asString();
		logger.info("Return nextpage url={} of url={}", nextPageUrl, url);
		return nextPageUrl;
	}
}
