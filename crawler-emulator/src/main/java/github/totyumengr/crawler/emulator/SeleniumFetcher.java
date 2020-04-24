package github.totyumengr.crawler.emulator;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebElement;
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
	
	@Value("${fetcher.emulator.initialDelay}")
	private int initialDelay;
	@Value("${fetcher.emulator.period}")
	private int period;
	@Value("${fetcher.emulator.remoteaddress}")
	private String remoteWebDriver;
	
	@Value("${fetcher.emulator.pageloadwaittime}")
	private int pageLoadWaitTime;
	
	@Value("${fetcher.emulator.taskmaxretrycount}")
	private int taskMaxRetryCount;
	
	private RemoteWebDriver instance;
	
	static class SearchScript {
		
		private String url;
		private String keyword;

		private String keyWordElementId;
		private String searchResultContainerElementId;
		private String recordXpath;
		private int pageDownCount;
		private String nextPageXpath;
		
		public String getUrl() {
			return url;
		}
		public void setUrl(String url) {
			this.url = url;
		}
		public String getKeyWordElementId() {
			return keyWordElementId;
		}
		public void setKeyWordElementId(String keyWordElementId) {
			this.keyWordElementId = keyWordElementId;
		}
		public String getKeyword() {
			return keyword;
		}
		public void setKeyword(String keyword) {
			this.keyword = keyword;
		}
		public String getSearchResultContainerElementId() {
			return searchResultContainerElementId;
		}
		public void setSearchResultContainerElementId(String searchResultContainerElementId) {
			this.searchResultContainerElementId = searchResultContainerElementId;
		}
		public String getRecordXpath() {
			return recordXpath;
		}
		public void setRecordXpath(String recordXpath) {
			this.recordXpath = recordXpath;
		}
		public int getPageDownCount() {
			return pageDownCount;
		}
		public void setPageDownCount(int pageDownCount) {
			this.pageDownCount = pageDownCount;
		}
		public String getNextPageXpath() {
			return nextPageXpath;
		}
		public void setNextPageXpath(String nextPageXpath) {
			this.nextPageXpath = nextPageXpath;
		}
		
		static SearchScript build(Task task) {
			
			SearchScript ss = Crawlers.GSON.fromJson(Crawlers.GSON.toJson(task.getEmulator()), SearchScript.class);
			Map<String, String> params = Crawlers.parseParams(task.getFromUrl());
			ss.setKeyword(params.get(Crawlers.SEARCH_KEYWORD));
			ss.setUrl(task.getFromUrl().replaceAll("Crawlers.SEARCH_KEYWORD=" + ss.getKeyword(), ""));
			
			return ss;
		}
	}
	
	private class EmulatorTask implements Runnable {
		
		private Map<String, Integer> currentRetryCount = new HashMap<String, Integer>();
		
		@Override
		public void run() {
			
			Object taskData = null;
			List<String> resultUrls = new ArrayList<String>();
			Task task;
			try {
//				logger.info("111");
				taskData = fetcherClient.getQueue(Crawlers.EMULATOR_BACKLOG).poll();
				if (taskData != null) {
					logger.info("Get a emulator task, script={}", taskData);
					task = Crawlers.GSON.fromJson(taskData.toString(), Task.class);
					SearchScript search = SearchScript.build(task);
					
					if (!currentRetryCount.containsKey(task.getFromUrl())) {
						currentRetryCount.put(task.getFromUrl(), 0);
					}
					
					try {
						DesiredCapabilities dc = DesiredCapabilities.chrome();
						dc.setCapability("pageLoadStrategy", "eager");
						instance = new RemoteWebDriver(new URL(remoteWebDriver), dc);
					} catch (Exception e) {
						throw new IllegalStateException("Can not connect a remote server. " + remoteWebDriver, e);
					}
					
					logger.info("Start to search it, {} {}", search.getUrl(), search.getKeyword());
					
					try {
						instance.get(search.getUrl());
						instance.findElement(By.id(search.getKeyWordElementId())).sendKeys(search.getKeyword() + Keys.ENTER);
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
							// 开始翻页
							WebElement nextPage = new WebDriverWait(instance, pageLoadWaitTime)
							        .until(ExpectedConditions.visibilityOfElementLocated(By.xpath(search.getNextPageXpath())));
							nextPage.click();
							searchResult = new WebDriverWait(instance, pageLoadWaitTime)
							        .until(ExpectedConditions.visibilityOfElementLocated(By.id(search.getSearchResultContainerElementId())));
							resultList = searchResult.findElements(By.xpath(search.getRecordXpath()));
							for (WebElement a : resultList) {
								String href = a.getAttribute("href");
								logger.info("Search result found. {}", href);
								resultUrls.add(href);
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
					fetcherClient.getMap(Crawlers.PREFIX_EXTRACT_DATA + task.getExtractor()).put(search.getUrl(), json);
		    		logger.info("Success to extract for url={}, push into {}", search.getUrl(), Crawlers.PREFIX_EXTRACT_DATA);
				}
			} catch (Exception e) {
				fetcherClient.getQueue(Crawlers.EMULATOR_BACKLOG).add(taskData);
				logger.error("Error occurred, push back to queue.", e);
			} finally {
				try {
					instance.quit();
				} catch (Exception ex) {
					// Ignore
				}
			}
		}
	}
	
	@PostConstruct
	public void init() {
		
		Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(new EmulatorTask(),
				initialDelay, period, TimeUnit.SECONDS);
		logger.info("Start to watch {}", Crawlers.EMULATOR_BACKLOG);
	}
}
