package github.totyumengr.crawler.extractor;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.redisson.api.RedissonClient;
import org.seimicrawler.xpath.JXDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import io.netty.buffer.ByteBufUtil;

/**
 * 从{@code #RAWDATA}中获取原始数据，然后调用指定的解析器进行解析。
 * @author mengran7
 *
 */
@Component
public class RawDataExtractor {

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	// Redis Keys ----------------------------------------
	public static final String RAWDATA = "rawdata";
	public static final String EXTRACTOR = "crawler.extractor";
	// ---------------------------------------------------
	
	public static final String URL = "url";
	public static final String CONTENT = "content";
	
	public static Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
	
	@Autowired
	private RedissonClient rawDataClient;
	
	@Autowired
	private ApplicationContext context;
	
	@Value("${extractor.initialDelay}")
	private int initialDelay;
	@Value("${extractor.period}")
	private int period;
	
	private String determineExtractor(String url) {
		
		// TODO: 后续改为规则型的匹配
		Object extractor = rawDataClient.getMap(EXTRACTOR).get(url);
		return extractor == null ? null : extractor.toString();
	}
	
	/**
	 * 启动
	 */
	@PostConstruct
	private void init() {
		
		// TODO: 考虑更高效的方式，比如Listener
		Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new ExtractWorker(),
				initialDelay, period, TimeUnit.SECONDS);
		logger.info("Start to watch {}", RAWDATA);
	}
	
	/**
	 * 内容提取器
	 * @author mengran7
	 *
	 */
	private class ExtractWorker implements Runnable {
		
		@Override
		public void run() {
			
			Object rawData = rawDataClient.getQueue(RAWDATA).poll();
			try {
				if (rawData != null) {
					Map<String, String> res = GSON.fromJson(rawData.toString(),
							new TypeToken<Map<String, String>>() {}.getType());
					if (!res.containsKey(URL)) {
						logger.info("Ignore illegal element={}", res.keySet());
						return;
					}
					// 确定解析器，如果没有使用默认的。
					String url = new String(ByteBufUtil.decodeHexDump(res.get(URL)), "UTF-8");
					String extractorType = determineExtractor(url);
					logger.info("Use {} to extractor content of url={}", extractorType, url);
					
					Extractor extractor = context.getBean(extractorType, Extractor.class);
					
					String content = new String(ByteBufUtil.decodeHexDump(res.get(CONTENT)), "UTF-8");
					boolean isSuccess = extractor.extract(url, JXDocument.create(content), res);
					logger.info("Is Done={}...extract content of url={}", isSuccess, url);
				}
			} catch (NoSuchBeanDefinitionException nsbde) {
				rawDataClient.getQueue(RAWDATA).offer(rawData);
				logger.error("UnSupport extractor type and put-back.", nsbde);
			} catch (Exception e) {
				logger.error("Extract error.", e);
			}
		}
	}
}
