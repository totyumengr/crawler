package github.totyumengr.crawler.fetcher.crawlers;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import cn.wanghaomiao.seimi.annotation.Crawler;
import cn.wanghaomiao.seimi.def.BaseSeimiCrawler;
import cn.wanghaomiao.seimi.http.SeimiHttpType;
import cn.wanghaomiao.seimi.spring.common.CrawlerCache;
import cn.wanghaomiao.seimi.struct.BodyType;
import cn.wanghaomiao.seimi.struct.Request;
import cn.wanghaomiao.seimi.struct.Response;

/**
 * 从{@code BACKLOG}获取抓取URL并执行抓取，如果抓取不成功则退回，根据策略继续重试，直到抓取成功为止。
 * 成功之后，将抓取到的内容推至{@code RAWDATA}队列。
 * @author mengran7
 *
 */
@Crawler(name = "backlog", delay = 1, httpType = SeimiHttpType.OK_HTTP3, httpTimeOut = 10000)
public class BackLogFetcher extends BaseSeimiCrawler {

	/**
	 * 序列化号
	 */
	private static final long serialVersionUID = 5343998991786617324L;
	
	public static final String BACKLOG = "backlog";
	public static final String RAWDATA = "rawdata";
	
	@Autowired
	private RedissonClient backlogClient;
	
	@Value("${backlog.initialDelay}")
	private int initialDelay;
	@Value("${backlog.period}")
	private int period;
	
	class FetchTask implements Runnable {
		
		protected Logger logger = LoggerFactory.getLogger(getClass());
		
		private RedissonClient backlogClient;
		
		public FetchTask(RedissonClient backlogClient) {
			this.backlogClient = backlogClient;
		}

		@Override
		public void run() {
			try {
				Object url = backlogClient.getQueue(BackLogFetcher.BACKLOG).poll();
				if (url != null) {
					logger.debug("Get a fetch task, url={}", url);
					Request req = Request.build(url.toString(), "handleResponse");
					req.setCrawlerName(BackLogFetcher.BACKLOG);
					CrawlerCache.consumeRequest(req);
					logger.info("Submit a fetch task, url={}", url);
				}
			} catch (Exception e) {
				logger.warn("Ignore unexpect error occurred: {}", e.getMessage());
			}
			
		}

	}
	
	/**
	 * 成功抓取的URL返回内容。
	 * @param response 获取的返回文档
	 */
	public void handleResponse(Response response) {
		
		logger.info("Success fetch url={}", response.getUrl());
		if (BodyType.TEXT.equals(response.getBodyType())) {
			// Push response to raw-data status
			Map<String, String> rawData = new HashMap<String, String>(1);
			rawData.put(response.getUrl(), response.getContent());
			backlogClient.getQueue(BackLogFetcher.RAWDATA).add(rawData);
			logger.info("Push into queue={} which response of url={}", BackLogFetcher.RAWDATA, response.getUrl());
		} else {
			logger.info("Ignore un-text response of url={}", response.getUrl());
		}
	}
	
	/**
	 * 启动时空跑。
	 */
	@Override
	public String[] startUrls() {
		
		Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new FetchTask(backlogClient),
				initialDelay, period, TimeUnit.SECONDS);
		logger.info("Start to watch backlog");
		
		return new String[0];
	}

	@Override
	public void start(Response response) {
		// 空方法，因为未设置启动URL。
	}

	/**
	 * 记录失败日志，并退回至Backlog
	 */
	@Override
	public void handleErrorRequest(Request request) {
		
		logger.info("Fail to fetch url={}", request.getUrl());
		backlogClient.getQueue(BACKLOG).add(request.getUrl());
		logger.info("Return url={} to backlog because fail to fetch", request.getUrl());
	}

}
