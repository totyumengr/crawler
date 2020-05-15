package github.totyumengr.crawler.emulator;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.RandomUtils;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.Proxy;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.remote.CapabilityType;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.github.monkeywie.proxyee.server.HttpProxyServer;

import github.totyumengr.crawler.Crawlers;
import github.totyumengr.crawler.Crawlers.Task;

/**
 * 通过连接Selenium Standalone Server，获取HTML结果。
 * <br/>
 * 由于技术限制，这里兼有Fetcher和Extractor两者的功能。
 * @author mengran7
 *
 */
@Component
public class SeleniumFetcher {

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Autowired
	private RedissonClient fetcherClient;
	
	@Value("${fetcher.emulator.remoteaddress}")
	private String remoteWebDriver;
	
	@Value("${fetcher.emulator.pageloadwaittime}")
	private int pageLoadWaitTime;
	
	@Value("${fetcher.emulator.taskmaxretrycount}")
	private int taskMaxRetryCount;
	
	@Value("${backlog.proxy.authName}")
	private String proxyUserName;
	@Value("${backlog.proxy.authPassword}")
	private String proxyUserPassword;
	
	@Value("${proxypool.forwardproxy.port}")
	private int serverPort;
	@Value("${proxypool.forwardproxy.ip}")
	private String serverIp;
	
	private String obtainProxyIP() {
		
		Set<Object> proxys = fetcherClient.getMap(Crawlers.PROXYPOOL).keySet();
		if (proxys != null && proxys.size() > 0) {
			Object[] ips = proxys.toArray();
			String useProxyIp = ips[RandomUtils.nextInt(0, ips.length)].toString();
			logger.info("Use proxy IP={} to build request.", useProxyIp);
			return useProxyIp.replaceAll("http://", "");
		} else {
			logger.info("Directly build request.");
			return null;
		}
	}
	
	private class ForwardProxyTask implements Runnable {

		private HttpProxyServer server;
		
		public ForwardProxyTask(HttpProxyServer server) {
			super();
			this.server = server;
		}

		@Override
		public void run() {
			
			server.start(serverPort);
		}
	}
	
	private class EmulatorTask implements Runnable {
		
		private Map<String, Integer> currentRetryCount = new HashMap<String, Integer>();
		
		@Override
		public void run() {
			
			while (true) {
				Object taskData = null;
				List<String> resultUrls = new ArrayList<String>();
				Task task;
				RemoteWebDriver instance = null;
				HttpProxyServer proxyServer = null;
				ExecutorService proxyServerExecutor = Executors.newSingleThreadExecutor();
				try {
					taskData = fetcherClient.getBlockingQueue(Crawlers.EMULATOR_BACKLOG).take();
					logger.info("Get a emulator task, script={}", taskData);
					
					task = Crawlers.GSON.fromJson(taskData.toString(), Task.class);
					SearchScript search = SearchScript.build(task);
					
					if (!currentRetryCount.containsKey(task.getFromUrl())) {
						currentRetryCount.put(task.getFromUrl(), 0);
					}
					
					try {
						DesiredCapabilities dc = DesiredCapabilities.chrome();
						dc.setCapability(CapabilityType.PAGE_LOAD_STRATEGY, "eager");
						if (serverIp != null) {
							
							ForwardProxy forwardProxy = new ForwardProxy(proxyUserName, proxyUserPassword);
							String proxyAddress = obtainProxyIP();
							String[] ipPort = (proxyAddress == null ? "" : proxyAddress).split(":");
							if (ipPort.length > 1) {
								proxyServer = forwardProxy.buildProxy(ipPort[0], Integer.valueOf(ipPort[1]));
								proxyServerExecutor.execute(new ForwardProxyTask(proxyServer));
								try {
									Thread.sleep(500);
								} catch (Exception ignore) {
									// Ignore
								}
								Proxy proxy = new Proxy();
							    proxy.setHttpProxy(serverIp + ":" + serverPort);
							    proxy.setSslProxy(serverIp + ":" + serverPort);
							    dc.setCapability(CapabilityType.PROXY, proxy);
							    logger.info("Proxy setted. {}", proxy.getHttpProxy());
							}
						}
						instance = new RemoteWebDriver(new URL(remoteWebDriver), dc);
					} catch (Exception e) {
						throw new IllegalStateException("Can not connect a remote server. " + remoteWebDriver, e);
					}
					
					logger.info("Start to search it, {} {}", search.getUrl(), search.getKeyword());
					
					try {
						// 打开网页
						instance.get(search.getUrl());
						try {
							Thread.sleep(500);
						} catch (Exception ignore) {
							// Ignore
						}
						
						WebElement keyword = new WebDriverWait(instance, pageLoadWaitTime)
						        .until(ExpectedConditions.visibilityOfElementLocated(By.id(search.getKeyWordElementId())));
						keyword.sendKeys(search.getKeyword() + Keys.ENTER);
						try {
							Thread.sleep(500);
						} catch (Exception ignore) {
							// Ignore
						}
						
						// 设置搜索工具
						if (search.getSearchToolXpath() != null) {
							WebElement searchTools = new WebDriverWait(instance, pageLoadWaitTime)
									.until(ExpectedConditions.presenceOfElementLocated(By.xpath(search.getSearchToolXpath())));
							searchTools.click();
							try {
								Thread.sleep(500);
							} catch (Exception ignore) {
								// Ignore
							}
							
							WebElement searchToolsOneMonth = new WebDriverWait(instance, pageLoadWaitTime)
							        .until(ExpectedConditions.presenceOfElementLocated(By.xpath(search.getTimeRangeXpath())));
							instance.executeScript("arguments[0].click();", searchToolsOneMonth);
							try {
								Thread.sleep(500);
							} catch (Exception ignore) {
								// Ignore
							}
						}
						WebElement searchResult = new WebDriverWait(instance, pageLoadWaitTime)
						        .until(ExpectedConditions.visibilityOfElementLocated(By.id(search.getSearchResultContainerElementId())));
						instance.executeScript("window.scrollTo(0, document.body.scrollHeight);");
						List<WebElement> resultList = searchResult.findElements(By.xpath(search.getRecordXpath()));
						for (WebElement a : resultList) {
							String href = a.getAttribute("href");
							logger.info("Search result found. {}", href);
							resultUrls.add(href);
						}
						
						int currentPageDownCount = 0;
						while(currentPageDownCount < search.getPageDownCount()) {
							currentPageDownCount++;
							logger.info("Next page, count {}", currentPageDownCount);
							instance.executeScript("window.scrollTo(0, document.body.scrollHeight);");
							
							for (int i =0; i < taskMaxRetryCount; i++) {
								try {
									// 开始翻页
									WebElement nextPage = new WebDriverWait(instance, pageLoadWaitTime)
									        .until(ExpectedConditions.visibilityOfElementLocated(By.xpath(search.getNextPageXpath())));
									nextPage.click();
									try {
										Thread.sleep(500);
									} catch (Exception ignore) {
										// Ignore
									}
									searchResult = new WebDriverWait(instance, pageLoadWaitTime)
									        .until(ExpectedConditions.visibilityOfElementLocated(By.id(search.getSearchResultContainerElementId())));
									resultList = searchResult.findElements(By.xpath(search.getRecordXpath()));
									for (WebElement a : resultList) {
										String href = a.getAttribute("href");
										logger.info("Search result found. {}", href);
										resultUrls.add(href);
									}
									logger.info("Success to retrive next-page at {}", i);
									break;
								} catch (Exception e) {
									// Continue;
									logger.info("Try to retrieve next-page at {}", i);
									try {
										Thread.sleep(500);
									} catch (Exception ignore) {
										// Ignore
									}
								}
							}
						}
					} catch (Exception e) {
						if (currentRetryCount.get(task.getFromUrl()) >= taskMaxRetryCount) {
							// 放弃
							logger.error("Give up {}", task.getFromUrl());
							currentRetryCount.remove(task.getFromUrl());
						} else {
							Integer c = currentRetryCount.get(task.getFromUrl());
							currentRetryCount.put(task.getFromUrl(), ++c);
							throw e;
						}
					}
					
					Map<String, Object> structData = new HashMap<String, Object>();
					List<List<String>> coreData = new ArrayList<List<String>>();
					structData.put(Crawlers.EXTRACT_DATA, coreData);
					for (String resultUrl : resultUrls) {
						List<String> l = new ArrayList<String>(1);
						l.add(resultUrl);
						coreData.add(l);
					}
					String json = Crawlers.GSON.toJson(structData);
					fetcherClient.getQueue(task.getStoryName() + Crawlers.EXTRACT_STRUCT_DATA + task.getExtractor() + search.getUrl()).add(json);
		    		logger.info("Success to extract for url={}, push into {}", search.getUrl(), Crawlers.EXTRACT_STRUCT_DATA);
				} catch (Exception e) {
					fetcherClient.getQueue(Crawlers.EMULATOR_BACKLOG).add(taskData);
					logger.error("Error occurred, push back to queue.", e);
				} finally {
					try {
						instance.quit();
						proxyServer.close();
						proxyServerExecutor.shutdownNow();
						Thread.sleep(1000);
					} catch (Exception ex) {
						// Ignore
						logger.info("Erro when finally emulator.", ex);
					}
				}
			}
		}
	}
	
	@PostConstruct
	public void init() {
		
		Executors.newSingleThreadExecutor().execute(new EmulatorTask());
		logger.info("Start to watch {}", Crawlers.EMULATOR_BACKLOG);
	}
}
