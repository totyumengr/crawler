package github.totyumengr.crawler.extractor;

import java.util.Map;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.redisson.api.RedissonClient;
import org.seimicrawler.xpath.JXDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.google.gson.reflect.TypeToken;

import github.totyumengr.crawler.Crawlers;
import io.netty.buffer.ByteBufUtil;

/**
 * 从{@code #RAWDATA}中获取原始数据，然后调用指定的解析器进行解析。
 * @author mengran7
 *
 */
@Component
public class RawDataExtractor {

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Autowired
	private RedissonClient rawDataClient;
	
	@Autowired
	private ApplicationContext context;
	
	private String determineExtractor(String url) {
		
		Object extractor = rawDataClient.getMap(Crawlers.EXTRACTOR).get(url);
		return extractor == null ? null : extractor.toString();
	}
	
	/**
	 * 启动
	 */
	@PostConstruct
	private void init() {
		
		Executors.newSingleThreadExecutor().submit(new ExtractWorker());
		logger.info("Start to watch {}", Crawlers.RAWDATA);
	}
	
	/**
	 * 内容提取器
	 * @author mengran7
	 *
	 */
	private class ExtractWorker implements Runnable {
		
		@Override
		public void run() {
			
			Object rawData = null;
			while (true) {
				try {
					rawData = rawDataClient.getBlockingQueue(Crawlers.RAWDATA).take();
					Map<String, String> res = Crawlers.GSON.fromJson(rawData.toString(),
							new TypeToken<Map<String, String>>() {}.getType());
					if (!res.containsKey(Crawlers.URL)) {
						logger.info("Ignore illegal element={}", res.keySet());
						return;
					}
					// 确定解析器，如果没有使用默认的。
					String url = new String(ByteBufUtil.decodeHexDump(res.get(Crawlers.URL)), "UTF-8");
					String extractorType = determineExtractor(url);
					logger.info("Use {} to extractor content of url={}", extractorType, url);
					
					// 处理REPOST
					String repostUrl = null;
					String repostCookie = null;
					if (res.containsKey(Crawlers.REPOST)) {
						repostUrl = new String(ByteBufUtil.decodeHexDump(res.get(Crawlers.REPOST)), "UTF-8");
						repostCookie = new String(ByteBufUtil.decodeHexDump(res.get(Crawlers.REPOST_COOKIE)), "UTF-8");
					}
					
					Extractor extractor = context.getBean(extractorType, Extractor.class);
					
					String content = new String(ByteBufUtil.decodeHexDump(res.get(Crawlers.CONTENT)), "UTF-8");
					logger.info("extract from...");
					logger.info("{}", content);
					
					boolean isSuccess = extractor.extract(url, JXDocument.create(content), extractorType, repostUrl, repostCookie);
					logger.info("Is Done={}...extract content of url={}", isSuccess, url);
				} catch (NoSuchBeanDefinitionException nsbde) {
					rawDataClient.getQueue(Crawlers.RAWDATA).offer(rawData);
					logger.error("UnSupport extractor type and put-back.", nsbde);
				} catch (Exception e) {
					logger.error("Extract error.", e);
				}
			}
		}
	}
}
