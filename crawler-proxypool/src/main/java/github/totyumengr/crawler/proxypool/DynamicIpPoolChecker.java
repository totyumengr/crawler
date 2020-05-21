package github.totyumengr.crawler.proxypool;

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
@Crawler(name = DynamicIpPoolChecker.IP_POOL_CHECKER, delay = 1, httpType = SeimiHttpType.APACHE_HC, httpTimeOut = 10000)
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
	
	@Value("${fetcher.ippool.minsize}")
	private int minSizeInPool;
	
	@Value("${backlog.proxy.authName}")
	private String proxyAuthenticatorName;
	@Value("${backlog.proxy.authPassword}")
	private String proxyAuthenticatorPassword;
	
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
					// 清楚原来的代理IP
					checkerClient.getMap(Crawlers.PROXYPOOL).clear();
					
					Request ippoolReq = Request.build(ippoolUrl, "handleResponse");
					ippoolReq.setCrawlerName(IP_POOL_CHECKER);
					ippoolReq.setSkipDuplicateFilter(true);
					ippoolReq.setMaxReqCount(1);
					
					ippoolReq.setProxyAuthenticatorName(proxyAuthenticatorName);
					ippoolReq.setProxyAuthenticatorPassword(proxyAuthenticatorPassword);
					
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
					
					checkReq.setProxyAuthenticatorName(proxyAuthenticatorName);
					checkReq.setProxyAuthenticatorPassword(proxyAuthenticatorPassword);
					
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
		
		try {
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
						// 清楚原来的
						checkerClient.getMap(Crawlers.PROXYPOOL).clear();
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
		} catch (Exception e) {
			logger.error("Error when try to handleResponse, {}", response.getRealUrl(), e);
		}
	}
	
	/**
	 * 启动时空跑。
	 */
	@Override
	public String[] startUrls() {
		
		Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new CheckerTask(),
				initialDelay, period, TimeUnit.SECONDS);
		
		Executors.newSingleThreadExecutor().execute(new Runnable() {

			@Override
			public void run() {
				while (true) {
					try {
						Object anti = checkerClient.getBlockingQueue(Crawlers.PROXYIP_ANTI).take();
						String proxyIp = anti.toString();
						if (checkerClient.getMap(Crawlers.PROXYPOOL).size() >= minSizeInPool) {
							checkerClient.getMap(Crawlers.PROXYPOOL).remove(proxyIp);
							logger.info("Found {} in anti-list, maybe is not available, remove it", proxyIp);
						}
					} catch (Exception e) {
						logger.info("Error when try to get anti proxy ip", e);
					}
				}				
			}
		});
		logger.info("Start to check...");
		
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
		
		try {
			String useProxyIp = PROXY_LOCAL.get();
			checkerClient.getMap(Crawlers.PROXYPOOL).remove(useProxyIp);
			logger.info("{} maybe is not available, remove it. url={}", useProxyIp, request.getUrl());
		} catch (Exception e) {
			logger.error("Error when try to handleErrorRequest, {}", request.getUrl(), e);
		}
		
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
		
		String wrongStyle = "~~http://1.2.3.4:1234";
		try {
			String useProxyIp = PROXY_LOCAL.get();
			if (useProxyIp == null) {
				Set<Object> proxys = checkerClient.getMap(Crawlers.PROXYPOOL).readAllKeySet();
				if (proxys != null && proxys.size() > 0) {
					Object[] ips = proxys.toArray();
					useProxyIp = ips[RandomUtils.nextInt(0, ips.length)].toString();
					PROXY_LOCAL.set(useProxyIp);
					logger.info("Use proxy IP={} to build request.", useProxyIp);
					return useProxyIp;
				}
			} else {
				logger.info("Use thread-local proxy IP={} to build request.", useProxyIp);
				return useProxyIp;
			}
			
		} catch (Exception e) {
			logger.warn("Ignore error when try to get a proxy IP", e);
			return wrongStyle;
		}
		
		return wrongStyle;
	}
}
