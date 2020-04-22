package github.totyumengr.crawler.fetcher.crawlers;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomUtils;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.google.gson.reflect.TypeToken;

import cn.wanghaomiao.seimi.annotation.Crawler;
import cn.wanghaomiao.seimi.def.BaseSeimiCrawler;
import cn.wanghaomiao.seimi.http.SeimiCookie;
import cn.wanghaomiao.seimi.http.SeimiHttpType;
import cn.wanghaomiao.seimi.spring.common.CrawlerCache;
import cn.wanghaomiao.seimi.struct.BodyType;
import cn.wanghaomiao.seimi.struct.Request;
import cn.wanghaomiao.seimi.struct.Response;
import github.totyumengr.crawler.Crawlers;
import io.netty.buffer.ByteBufUtil;

/**
 * 从{@code BACKLOG}获取抓取URL并执行抓取，如果抓取不成功则退回，根据策略继续重试，直到抓取成功为止。
 * 成功之后，将抓取到的内容推至{@code RAWDATA}队列。
 * <p>
 * 支持动态代理IP池：{@code PROXYPOOL}
 * @author mengran7
 *
 */
@Crawler(name = Crawlers.BACKLOG, delay = 1, httpType = SeimiHttpType.OK_HTTP3, httpTimeOut = 10000)
public class BackLogFetcher extends BaseSeimiCrawler {

	/**
	 * 序列化号
	 */
	private static final long serialVersionUID = 5343998991786617324L;
	
	private final static ThreadLocal<String> PROXY_LOCAL = new ThreadLocal<String>();
	
	@Autowired
	private RedissonClient fetcherClient;
	
	@Value("${backlog.initialDelay}")
	private int initialDelay;
	@Value("${backlog.period}")
	private int period;
	@Value("${backlog.proxy.authName}")
	private String proxyAuthenticatorName;
	@Value("${backlog.proxy.authPassword}")
	private String proxyAuthenticatorPassword;
	
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
				Object url = backlogClient.getQueue(Crawlers.BACKLOG).poll();
				if (url != null) {
					logger.debug("Get a fetch task, url={}", url);
					Request req = Request.build(url.toString(), "handleResponse");
					req.setCrawlerName(Crawlers.BACKLOG);
					req.setSkipDuplicateFilter(true);

					Object cookieList = backlogClient.getMap(Crawlers.COOKIES).get(req.getUrl());
					if (cookieList != null) {
						List<SeimiCookie> seimiCookie = Crawlers.GSON.fromJson(cookieList.toString(),
					            new TypeToken<List<SeimiCookie>>() {}.getType());
					    req.setSeimiCookies(seimiCookie);
					}

					req.setProxyAuthenticatorName(proxyAuthenticatorName);
					req.setProxyAuthenticatorPassword(proxyAuthenticatorPassword);

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
		
		boolean maybe302 = false;
		try {
			URL o = new URL(response.getUrl());
			URL r = new URL(response.getRealUrl());
			if (!o.getHost().equals(r.getHost()) || !o.getPath().equals(r.getPath())) {
				logger.info("Found 302 event, original url]{}, realUrl={}", response.getUrl(), response.getRealUrl());
				maybe302 = true;
			}
 		} catch (Exception e) {
 			logger.error("Ignore 302 check...");
 		}
		boolean needRepush = false;
		boolean needResubmit = false;
		Map<String, String> needAppendParams = new LinkedHashMap<String, String>();
		if (maybe302) {
			List<SeimiCookie> seimiCookies = response.getRequest().getSeimiCookies();
			Map<String, String> params = Crawlers.parseParams(response.getRealUrl());
			Map<String, String> oriParams = Crawlers.parseParams(response.getUrl());
			if (seimiCookies != null) {
				// Cookie问题
				for (SeimiCookie cookie : seimiCookies) {
					if (params.containsKey(cookie.getName())) {
						cookie.setValue(params.get(cookie.getName()));
						needRepush = true;
					}
				}
			}
			// 将多出来的参数附加到原参数列表中
			for (Entry<String, String> entry : params.entrySet()) {
				if (!oriParams.containsKey(entry.getKey())) {
					needAppendParams.put(entry.getKey(), entry.getValue());
					needResubmit = true;
				}
			}
		}

		String newUrl = "";
		List<SeimiCookie> newCookie = new ArrayList<SeimiCookie>();
		if (needRepush && !needResubmit) {
			// 直接Re-Send request操作，因为URL没有变化
			CrawlerCache.consumeRequest(response.getRequest());
			logger.info("Because 302 cookie updated, re-submit a fetch task, url={}", response.getUrl());
			return;
		}
		
		logger.info("Success fetch url={}", response.getUrl());
		if (BodyType.TEXT.equals(response.getBodyType())) {
			// Push response to raw-data status
			Map<String, String> rawData = new HashMap<String, String>(2);
			String hexUrl = ByteBufUtil.hexDump(response.getUrl().getBytes("UTF-8"));
			rawData.put(Crawlers.URL, hexUrl);
			String hexContent = ByteBufUtil.hexDump(response.getContent().getBytes("UTF-8"));
			rawData.put(Crawlers.CONTENT, hexContent);
			
			// TODO: 没有放开Re-push的口，还不支持
			if (needRepush) {
				newUrl = Crawlers.appendParams(response.getRequest().getUrl(), needAppendParams);
				newCookie = response.getRequest().getSeimiCookies();
				logger.warn("!!! Need re-push={} with cookie{}, but un-supported.!!!", newUrl, newCookie);
//				logger.info("Repost request because 302 change cookie.");
//				rawData.put(Crawlers.REPOST, ByteBufUtil.hexDump(newUrl.getBytes("UTF-8")));
//				rawData.put(Crawlers.REPOST_COOKIE, ByteBufUtil.hexDump(newCookie.toString().getBytes("UTF-8")));
			}
			
			fetcherClient.getQueue(Crawlers.RAWDATA).add(rawData);
			logger.info("Push into queue={} which response of url={}", Crawlers.RAWDATA, response.getUrl());
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
		Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new FetchTask(fetcherClient),
				initialDelay, period, TimeUnit.SECONDS);
		logger.info("Start to watch {}", Crawlers.BACKLOG);
		
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
		fetcherClient.getQueue(Crawlers.BACKLOG).add(request.getUrl());
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
				Object proxys = fetcherClient.getMap(Crawlers.PROXYPOOL).get(Crawlers.BACKLOG);
				if (proxys != null) {
					String[] ips = proxys.toString().split("\\|");
					useProxyIp = ips[RandomUtils.nextInt(0, ips.length)];
					PROXY_LOCAL.set(useProxyIp);
					logger.info("Use proxy IP={} to build request on {}.", useProxyIp, Crawlers.BACKLOG);
					return useProxyIp;
				}
			} else {
				logger.info("Use thread-local proxy IP={} to build request on {}.", useProxyIp, Crawlers.BACKLOG);
				return useProxyIp;
			}
			
		} catch (Exception e) {
			logger.warn("Ignore error when try to get a proxy IP", e);
			return null;
		}
		
		return null;
	}

}
