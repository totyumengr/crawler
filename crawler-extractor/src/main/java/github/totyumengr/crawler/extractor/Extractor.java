package github.totyumengr.crawler.extractor;

/**
 * 
 * 内容提取器接口定义
 * @author mengran7
 *
 */
public interface Extractor {
	
	/**
	 * 
	 * @param storyName 抓取源的批次
	 * @param url 抓取源
	 * @param html 抓取到的内容文档对象
	 * @param 解析结果
	 * @return 是否成功提取到结果
	 */
	boolean extract(String storyName, String url, String html, String extractorType, String status, String ip);
}
