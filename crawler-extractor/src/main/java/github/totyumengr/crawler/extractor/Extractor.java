package github.totyumengr.crawler.extractor;

import org.seimicrawler.xpath.JXDocument;

/**
 * 
 * 内容提取器接口定义
 * @author mengran7
 *
 */
public interface Extractor {
	
	/**
	 * 
	 * @param url 抓取源
	 * @param document 抓取到的内容文档对象
	 * @param 解析结果
	 * @return 是否成功提取到结果
	 */
	boolean extract(String url, JXDocument document, String extractorType);
}
