package github.totyumengr.crawler.fetcher.crawlers;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executors;

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
import cn.wanghaomiao.seimi.struct.CrawlerModel;
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
@Crawler(name = Crawlers.BACKLOG, delay = 1, httpType = SeimiHttpType.APACHE_HC, httpTimeOut = 10000, useCookie = true)
public class BackLogFetcher extends BaseSeimiCrawler {

	/**
	 * 序列化号
	 */
	private static final long serialVersionUID = 5343998991786617324L;
	
	private final static ThreadLocal<String> PROXY_LOCAL = new ThreadLocal<String>();
	
	@Autowired
	private RedissonClient fetcherClient;
	
	@Value("${backlog.proxy.authName}")
	private String proxyAuthenticatorName;
	@Value("${backlog.proxy.authPassword}")
	private String proxyAuthenticatorPassword;
	
	@Value("${backlog.repush.maxcount}")
	private int repushMaxCount;
	
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
			
			while(true) {
				try {
					Object backlog = backlogClient.getBlockingQueue(Crawlers.BACKLOG).take();
					logger.debug("Get a fetch task, url={}", backlog);
					
					List<String> json = Crawlers.GSON.fromJson(backlog.toString(), new TypeToken<List<String>>() {}.getType());
					String url = json.get(1);
					String storyName = json.get(0);
					
					Request req = Request.build(url, "handleResponse");
					req.setCrawlerName(Crawlers.BACKLOG);
					req.setSkipDuplicateFilter(true);
					req.setHeader(new HashMap<String, String>(2));
					req.getHeader().put("Connection", "close");
					req.getHeader().put("Accept-Encoding", "identity");
					
					req.getMeta().put(Crawlers.STORY_NAME, storyName);

					Object cookieList = backlogClient.getMap(Crawlers.COOKIES).get(req.getUrl());
					if (cookieList != null) {
						List<SeimiCookie> seimiCookie = Crawlers.GSON.fromJson(cookieList.toString(),
					            new TypeToken<List<SeimiCookie>>() {}.getType());
					    req.setSeimiCookies(seimiCookie);
					}

					req.setProxyAuthenticatorName(proxyAuthenticatorName);
					req.setProxyAuthenticatorPassword(proxyAuthenticatorPassword);
					
					CrawlerModel model = CrawlerCache.getCrawlerModel(Crawlers.BACKLOG);
					boolean consumed = true;
					do {
						consumed = CrawlerCache.tryConsumeRequest(req);
						logger.info("Try to push request url={}, result={}", url, consumed);
						if (!consumed) {
							logger.info("Slowdown...Cannot consume request, current Queue info is={}", model.queueInfo());
							// 队列已经满了。
							Thread.sleep(60 * 1000);
						}
					} while (!consumed);
					
					logger.info("Submit a fetch task, url={}", url);
				} catch (Exception e) {
					logger.warn("Ignore unexpect error occurred.", e);
				}
			}
		}
	}
	
	private static final String EMPTY_COUNT = "<html><body>NO_CONTENT</body></html>";
	
	/**
	 * 成功抓取的URL返回内容。
	 * @param response 获取的返回文档
	 */
	public void handleResponse(Response response) throws Exception {
		
		Object storyNameInMeta = response.getRequest().getMeta().get(Crawlers.STORY_NAME);
		String storyName = storyNameInMeta == null ? "" : storyNameInMeta.toString();
		
		try {
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

			if (needRepush && !needResubmit) {
				// 直接Re-Send request操作，因为URL没有变化
				CrawlerCache.consumeRequest(response.getRequest());
				logger.info("Because 302 cookie updated, re-submit a fetch task, url={}", response.getUrl());
				return;
			}
			
			logger.info("Success fetch url={}", response.getUrl());
			
			if (BodyType.TEXT.equals(response.getBodyType())) {
				doResponse(storyName, response.getUrl(), response.getContent());
			} else {
				logger.info("Ignore un-text response of url={}", response.getUrl());
				doResponse(storyName, response.getUrl(), null);
			}
		} catch (Exception e) {
			logger.error("Error when try to handleResponse, {}", response.getRealUrl(), e);
			doResponse(storyName, response.getUrl(), null);
		}
	}
	
	private void doResponse(String storyName, String url, String content) throws Exception {
		// Push response to raw-data status
		Map<String, String> rawData = new HashMap<String, String>(2);
		String hexUrl = ByteBufUtil.hexDump(url.getBytes("UTF-8"));
		rawData.put(Crawlers.URL, hexUrl);
		String realContent = content;
		if (realContent == null) {
			realContent = EMPTY_COUNT;
		}
		String hexContent = ByteBufUtil.hexDump(realContent.getBytes("UTF-8"));
		rawData.put(Crawlers.CONTENT, hexContent);
		
		rawData.put(Crawlers.STORY_NAME, storyName);
		
		// TODO: 没有放开Re-push的口，还不支持
//			logger.info("Repost request because 302 change cookie.");
//			rawData.put(Crawlers.REPOST, ByteBufUtil.hexDump(newUrl.getBytes("UTF-8")));
//			rawData.put(Crawlers.REPOST_COOKIE, ByteBufUtil.hexDump(newCookie.toString().getBytes("UTF-8")));
		
		fetcherClient.getQueue(Crawlers.RAWDATA).add(rawData);
		logger.info("Push into queue={} which response of url={}", Crawlers.RAWDATA, url);
	}
	
	
	/**
	 * 启动时空跑。
	 */
	@Override
	public String[] startUrls() {
		
		// TODO: 需要改一下，但取回任务到本地有丢失风险。
		Executors.newSingleThreadExecutor().execute(new FetchTask(fetcherClient));
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
		
		Object storyNameInMeta = request.getMeta().get(Crawlers.STORY_NAME);
		String storyName = storyNameInMeta == null ? "" : storyNameInMeta.toString();
		
		try {
			Object repushCount = fetcherClient.getMap(Crawlers.BACKLOG_REPUSH + storyName).addAndGet(request.getUrl(), 1);
			if (Integer.valueOf(repushCount.toString()) < repushMaxCount) {
				List<String> backlog = new ArrayList<String>(2);
				backlog.add(storyName);
				backlog.add(request.getUrl());
				fetcherClient.getQueue(Crawlers.BACKLOG).add(Crawlers.GSON.toJson(backlog));
				logger.info("Return url={} to backlog because fail to fetch. {}", request.getUrl(), repushCount);
			} else {
				fetcherClient.getMap(Crawlers.BACKLOG_REPUSH + storyName).put(request.getUrl(), 0);
				doResponse(storyName, request.getUrl(), EMPTY_COUNT);
				logger.info("Give up Fail to fetch url={}", request.getUrl());
			}
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
				Set<Object> proxys = fetcherClient.getMap(Crawlers.PROXYPOOL).keySet();
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
