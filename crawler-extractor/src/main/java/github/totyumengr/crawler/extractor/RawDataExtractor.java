package github.totyumengr.crawler.extractor;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.redisson.api.RedissonClient;
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
	
	private String determineExtractor(String storyName, String url) {
		
		Object extractor = rawDataClient.getMap(storyName + Crawlers.EXTRACTOR).get(url);
		return extractor == null ? null : extractor.toString();
	}
	
	private ExecutorService executor = Executors.newSingleThreadExecutor();
	/**
	 * 启动
	 */
	@PostConstruct
	private void init() {
		
		executor.submit(new ExtractWorker());
		logger.info("Start to watch {}", Crawlers.RAWDATA);
	}
	
	/**
	 * 关闭
	 */
	@PreDestroy
	private void destory() {
		// 设置信号量
		running = false;
		logger.info("Do destory logic {}", Crawlers.RAWDATA);
		try {
			Thread.sleep(3 * 1000);
			executor.shutdownNow();
		} catch (Exception e) {
			// Ignore
		}
		logger.info("Stop to watch {}", Crawlers.RAWDATA);
	}
	
	private volatile boolean running = true;
	
	/**
	 * 内容提取器
	 * @author mengran7
	 *
	 */
	private class ExtractWorker implements Runnable {
		
		@Override
		public void run() {
			
			Object rawData = null;
			while (running) {
				try {
					rawData = rawDataClient.getBlockingQueue(Crawlers.RAWDATA).take();
					Map<String, String> res = Crawlers.GSON.fromJson(rawData.toString(),
							new TypeToken<Map<String, String>>() {}.getType());
					if (!res.containsKey(Crawlers.URL)) {
						logger.info("Ignore illegal element={}", res.keySet());
						return;
					}
					
					String storyName = res.get(Crawlers.STORY_NAME);
					
					// 确定解析器，如果没有使用默认的。
					String url = new String(ByteBufUtil.decodeHexDump(res.get(Crawlers.URL)), "UTF-8");
					String extractorType = determineExtractor(storyName, url);
					logger.info("Use {} to extractor content of url={}", extractorType, url);
					
					Extractor extractor = context.getBean(extractorType, Extractor.class);
					
					String content = new String(ByteBufUtil.decodeHexDump(res.get(Crawlers.CONTENT)), "UTF-8");
					logger.info("extract from content {}", content.length());
					
					String status = res.get(Crawlers.FETCHER_FAIL_STATUS);
					String ip = res.get(Crawlers.FETCHER_PROXYIP);
					
					boolean isSuccess = extractor.extract(storyName, url, content, extractorType, status, ip);
					logger.info("Is Done={}...extract content of url={}", isSuccess, url);
				} catch (NoSuchBeanDefinitionException nsbde) {
					logger.error("UnSupport extractor type and put-back.", nsbde);
				} catch (Exception e) {
					logger.error("Extract error.", e);
				}
			}
		}
	}
}
