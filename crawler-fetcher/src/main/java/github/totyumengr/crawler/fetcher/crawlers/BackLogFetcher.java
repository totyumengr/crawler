package github.totyumengr.crawler.fetcher.crawlers;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomUtils;
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
import io.netty.buffer.ByteBufUtil;

/**
 * 从{@code BACKLOG}获取抓取URL并执行抓取，如果抓取不成功则退回，根据策略继续重试，直到抓取成功为止。
 * 成功之后，将抓取到的内容推至{@code RAWDATA}队列。
 * <p>
 * 支持动态代理IP池：{@code PROXYPOOL}
 * @author mengran7
 *
 */
@Crawler(name = BackLogFetcher.BACKLOG, delay = 1, httpType = SeimiHttpType.OK_HTTP3, httpTimeOut = 10000)
public class BackLogFetcher extends BaseSeimiCrawler {

	/**
	 * 序列化号
	 */
	private static final long serialVersionUID = 5343998991786617324L;
	
	// Redis Keys ----------------------------------------
	public static final String BACKLOG = "backlog";
	public static final String RAWDATA = "rawdata";
	public static final String PROXYPOOL = "proxypool";
	// ---------------------------------------------------
	
	public static final String URL = "url";
	public static final String CONTENT = "content";
	public static final String EXTRACTOR = "extractor";
	
	private final static ThreadLocal<String> PROXY_LOCAL = new ThreadLocal<String>();
	
	@Autowired
	private RedissonClient fetcherClient;
	
	@Value("${backlog.initialDelay}")
	private int initialDelay;
	@Value("${backlog.period}")
	private int period;
	
	/**
	 * 真正去执行的抓取任务类
	 * @author mengran7
	 *
	 */
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
					req.setSkipDuplicateFilter(true);
					Map<String, String> headers = new HashMap<String, String>(1);
					headers.put("Connection", "close");
					req.setHeader(headers);
					CrawlerCache.consumeRequest(req);
					logger.info("Submit a fetch task, url={}", url);
				}
			} catch (Exception e) {
				logger.warn("Ignore unexpect error occurred.", e);
			}
		}
	}
	
	/**
	 * 成功抓取的URL返回内容。
	 * @param response 获取的返回文档
	 */
	public void handleResponse(Response response) throws Exception {
		
		// Clear thread-local
		PROXY_LOCAL.set(null);
		
		logger.info("Success fetch url={}", response.getUrl());
		if (BodyType.TEXT.equals(response.getBodyType())) {
			// Push response to raw-data status
			Map<String, String> rawData = new HashMap<String, String>(2);
			String hexUrl = ByteBufUtil.hexDump(response.getUrl().getBytes("UTF-8"));
			rawData.put(URL, hexUrl);
			String hexContent = ByteBufUtil.hexDump(response.getContent().getBytes("UTF-8"));
			rawData.put(CONTENT, hexContent);
			fetcherClient.getQueue(BackLogFetcher.RAWDATA).add(rawData);
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
		
		// TODO: 需要改一下，但取回任务到本地有丢失风险。
		Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new FetchTask(fetcherClient),
				initialDelay, period, TimeUnit.SECONDS);
		logger.info("Start to watch {}", BACKLOG);
		
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
		
		// Clear thread-local
		PROXY_LOCAL.set(null);
		
		logger.info("Fail to fetch url={}", request.getUrl());
		fetcherClient.getQueue(BACKLOG).add(request.getUrl());
		logger.info("Return url={} to backlog because fail to fetch", request.getUrl());
	}

	/**
	 * 实现代理IP池
	 */
	@Override
	public String proxy() {
		
		try {
			String useProxyIp = PROXY_LOCAL.get();
			if (useProxyIp == null) {
				Object proxys = fetcherClient.getMap(PROXYPOOL).get(BACKLOG);
				if (proxys != null) {
					String[] ips = proxys.toString().split("\\|");
					useProxyIp = ips[RandomUtils.nextInt(0, ips.length)];
					PROXY_LOCAL.set(useProxyIp);
					logger.info("Use proxy IP={} to build request on {}.", useProxyIp, BACKLOG);
					return useProxyIp;
				}
			} else {
				logger.info("Use thread-local proxy IP={} to build request on {}.", useProxyIp, BACKLOG);
				return useProxyIp;
			}
			
		} catch (Exception e) {
			logger.warn("Ignore error when try to get a proxy IP", e);
			return null;
		}
		
		return null;
	}

}