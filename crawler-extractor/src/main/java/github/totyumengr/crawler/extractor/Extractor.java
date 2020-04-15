package github.totyumengr.crawler.extractor;

import java.util.Map;

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
	 * @param rawData 抓取器拿到的原始结果，Value是HEX字符串
	 * @return 是否成功提取到结果
	 */
	boolean extract(String url, JXDocument document, Map<String, String> rawData);
}
