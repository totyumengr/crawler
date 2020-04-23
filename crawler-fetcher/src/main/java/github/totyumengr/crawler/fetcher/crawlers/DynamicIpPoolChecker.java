package github.totyumengr.crawler.fetcher.crawlers;

import java.util.Set;
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
import github.totyumengr.crawler.Crawlers;

/**
 * 访问百度来确定代理IP的可用性，如不可用，移除。
 * <p>
 * @author mengran7
 *
 */
@Crawler(name = DynamicIpPoolChecker.IP_POOL_CHECKER, delay = 1, httpType = SeimiHttpType.OK_HTTP3, httpTimeOut = 10000)
public class DynamicIpPoolChecker extends BaseSeimiCrawler {

	/**
	 * 序列化号
	 */
	private static final long serialVersionUID = 5343998991786617324L;
	
	private final static ThreadLocal<String> PROXY_LOCAL = new ThreadLocal<String>();
	public static final String IP_POOL_CHECKER = "fetcher.ippool.checker";
	
	@Autowired
	private RedissonClient checkerClient;
	
	@Value("${fetcher.ippool.initialDelay}")
	private int initialDelay;
	@Value("${fetcher.ippool.period}")
	private int period;
	
	@Value("${fetcher.ippool.url}")
	private String ippoolUrl;
	
	@Value("${fetcher.ippool.okip.url}")
	private String checkerUrl;
	
	@Value("${fetcher.ippool.fullupdate.afternchecker}")
	private int afterNChecker;
	
	private int count;
	
	/**
	 * 真正去执行的抓取任务类
	 * @author mengran7
	 *
	 */
	class CheckerTask implements Runnable {
		
		protected Logger logger = LoggerFactory.getLogger(getClass());

		@Override
		public void run() {
			
			try {
				// 第一次 或者 N轮空次后
				if (count == 0 || count >= afterNChecker) {
					Request ippoolReq = Request.build(ippoolUrl, "handleResponse");
					ippoolReq.setCrawlerName(IP_POOL_CHECKER);
					ippoolReq.setSkipDuplicateFilter(true);
					ippoolReq.setMaxReqCount(1);
					CrawlerCache.consumeRequest(ippoolReq);
					logger.info("Submit a request, url={}", ippoolUrl);
					
					// Reset 计数器
					count = 0;
				}
				
				// 首次轮空
				if (count != 0) {
					Request checkReq = Request.build(checkerUrl, "handleResponse");
					checkReq.setCrawlerName(IP_POOL_CHECKER);
					checkReq.setSkipDuplicateFilter(true);
					checkReq.setMaxReqCount(1);
					CrawlerCache.consumeRequest(checkReq);
					logger.info("Submit a request, url={}", checkerUrl);
				}
			} catch (Exception e) {
				logger.warn("Ignore unexpect error occurred.", e);
			} finally {
				count++;
			}
		}
	}
	
	/**
	 * 成功抓取的URL返回内容。
	 * @param response 获取的返回文档
	 */
	public void handleResponse(Response response) throws Exception {
		
		if (response.getUrl().equals(checkerUrl)) {
			// 如果是IP可用检查的返回
			String useProxyIp = PROXY_LOCAL.get();
			logger.info("Proxy IP is available {}", useProxyIp);
		} else if (response.getUrl().equals(ippoolUrl)) {
			// 如果是全量IP获取的返回
			logger.info("Success fetch url={}", response.getUrl());
			if (BodyType.TEXT.equals(response.getBodyType())) {
				String ippool = response.getContent();
				String[] ips = ippool.split("\r\n");
				if (ips != null && ips.length > 0) {
					for (String ip : ips) {
						String proxyIp = "http://" + ip; 
						checkerClient.getMap(Crawlers.PROXYPOOL).put(proxyIp, proxyIp);
					}
				}
				logger.info("Update {} by {}", Crawlers.PROXYPOOL, response.getUrl());
			} else {
				logger.info("Ignore un-text response of url={}", response.getUrl());
			}
		}
	}
	
	/**
	 * 启动时空跑。
	 */
	@Override
	public String[] startUrls() {
		
		Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new CheckerTask(),
				initialDelay, period, TimeUnit.SECONDS);
		logger.info("Start to watch {}", Crawlers.BACKLOG);
		
		return new String[0];
	}

	@Override
	public void start(Response response) {
		// 空方法
	}

	/**
	 * 记录失败日志，并退回至Backlog
	 */
	@Override
	public void handleErrorRequest(Request request) {
		
		String useProxyIp = PROXY_LOCAL.get();
		checkerClient.getMap(Crawlers.PROXYPOOL).remove(useProxyIp);
		logger.info("{} maybe is not available, remove it. url={}", useProxyIp, request.getUrl());
	}
	
	@Override
    public void finallyRequest(Request request) {
		
		// Clear thread-local
		PROXY_LOCAL.set(null);
    }

	/**
	 * 实现代理IP池
	 */
	@Override
	public String proxy() {
		
		try {
			String useProxyIp = PROXY_LOCAL.get();
			if (useProxyIp == null) {
				Set<Object> proxys = checkerClient.getMap(Crawlers.PROXYPOOL).keySet();
				if (proxys != null && proxys.size() > 0) {
					Object[] ips = proxys.toArray();
					useProxyIp = ips[RandomUtils.nextInt(0, ips.length)].toString();
					PROXY_LOCAL.set(useProxyIp);
					return useProxyIp;
				} else {
					logger.warn("Not have available a proxy ip....");
				}
			} else {
				return useProxyIp;
			}
			
		} catch (Exception e) {
			logger.warn("Ignore error when try to get a proxy IP", e);
			return null;
		}
		
		return null;
	}

}
