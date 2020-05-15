package github.totyumengr.crawler;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.ImmutablePair;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

/**
 * 一些常量定义
 * @author mengran7
 *
 */
public final class Crawlers {
	
	public static ImmutablePair<List<String>, List<String>> clearDataKeys() {
		
		return new ImmutablePair<List<String>, List<String>> (
				Arrays.asList(
					Crawlers.EXTRACTOR_CONTENT_ANTI_ALERT, 
					Crawlers.BACKLOG_REPUSH, 
					Crawlers.XPATH_LIST_ELEMENTS, 
					Crawlers.XPATH_RECORD_ELEMENTS, 
					Crawlers.XPATH_PAGINGBAR_ELEMENTS, 
					Crawlers.XPATH_PAGINGBAR_NEXTURL_ELEMENTS,
					Crawlers.XPATH_CONTENT,
					Crawlers.XPATH_CONTENT_ANTI,
					Crawlers.EXTRACTOR,
					Crawlers.STORY_PIPELINE
				), 
				Arrays.asList(
					Crawlers.STORY_TRACE
				)
			);
	}

	// Redis Keys ----------------------------------------
	// Queue
	public static final String BACKLOG = "crawler.backlog";
	public static final String EMULATOR_BACKLOG = "crawler.backlog.emulator";
	public static final String RAWDATA = "crawler.rawdata";
	public static final String EXTRACT_STRUCT_DATA = "crawler.structdata";
	public static final String STORY_FILE_QUEYE = "worker.story";
	public static final String STORY_FILE_QUEYE_DONE = "worker.story.done";
	
	// 无需清除
	public static final String RECYCLE_BIN = "crawer.recyclebin";
	public static final String PROXYPOOL = "crawler.proxypool";
	public static final String STORY_TASKS = "worker.story.tasks";
	
	// Map
	public static final String BACKLOG_REPUSH = "crawler.backlog.repush";
	public static final String EXTRACTOR = "crawler.extractor";
	public static final String EXTRACTOR_CONTENT_ANTI_ALERT = "crawler.extractor.anti.alert";
	public static final String XPATH_LIST_ELEMENTS = "extractor.paging.list";
	public static final String XPATH_RECORD_ELEMENTS = "extractor.paging.list.record";
	public static final String XPATH_PAGINGBAR_ELEMENTS = "extractor.paging.bar";
	public static final String XPATH_PAGINGBAR_NEXTURL_ELEMENTS = "extractor.paging.bar.nexturl";
	public static final String XPATH_CONTENT = "extractor.content";
	public static final String XPATH_CONTENT_ANTI = "extractor.content.anti";
	public static final String STORY_PIPELINE = "worker.pipeline";
	
	// ListMultiMap
	public static final String STORY_TRACE = "worker.trace";
	// ---------------------------------------------------
	
	public static final String URL = "url";
	public static final String CONTENT = "content";
	public static final String STORY_NAME = "storyName";	
	public static final String REPOST = "repost";
	public static final String REPOST_COOKIE = "repost.cookie";
	public static final String SEARCH_KEYWORD = "_kw_";
	
	public static final String TASK_TEMPLATE = "template";
	public static final String TASK_PARAMS = "params";
	public static final String TASK_PARAMS_ARGS = "args";
	public static final String TASK_PARAMS_PIPELINE = "pipeline";
	
	public static final String EXTRACT_DATA = "structdata";
	public static final String PLEASE_SET_EXTRACT_XPATH = "PLEASE_SET_EXTRACT_XPATH";
	
	public static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
	
	public static final class Story {
		
		private String name;
		private List<Map<String, String>> tasks;
		private List<String> args;
		private String argsEL;
		
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public List<Map<String, String>> getTasks() {
			return tasks;
		}
		public void setTasks(List<Map<String, String>> tasks) {
			this.tasks = tasks;
		}
		public List<String> getArgs() {
			return args;
		}
		public void setArgs(List<String> args) {
			this.args = args;
		}
		public String getArgsEL() {
			return argsEL;
		}
		public void setArgsEL(String argsEL) {
			this.argsEL = argsEL;
		}
	}
	
	public static class Task {
		
		private String name;
		private String fromUrl;
		private String extractor;
		private Map<String, String> extractRules;
		private boolean pageDown;
		private String landing;
		private List<Map<String, String>> cookies;
		private int pageDownCount;
		// 相当于Bin-log
		private boolean traceLog = true;
		private Map<String, String> emulator;
		
		public Map<String, String> getEmulator() {
			return emulator;
		}
		public void setEmulator(Map<String, String> emulator) {
			this.emulator = emulator;
		}
		public int getPageDownCount() {
			return pageDownCount;
		}
		public void setPageDownCount(int pageDownCount) {
			this.pageDownCount = pageDownCount;
		}
		public List<Map<String, String>> getCookies() {
			return cookies;
		}
		public void setCookies(List<Map<String, String>> cookies) {
			this.cookies = cookies;
		}
		public Map<String, String> getExtractRules() {
			return extractRules;
		}
		public void setExtractRules(Map<String, String> extractRules) {
			this.extractRules = extractRules;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getFromUrl() {
			return fromUrl;
		}
		public void setFromUrl(String fromUrl) {
			this.fromUrl = fromUrl;
		}
		public String getExtractor() {
			return extractor;
		}
		public void setExtractor(String extractor) {
			this.extractor = extractor;
		}
		public boolean isPageDown() {
			return pageDown;
		}
		public void setPageDown(boolean pageDown) {
			this.pageDown = pageDown;
		}
		public String getLanding() {
			return landing;
		}
		public void setLanding(String landing) {
			this.landing = landing;
		}
		
		public boolean isTraceLog() {
			return traceLog;
		}
		public void setTraceLog(boolean traceLog) {
			this.traceLog = traceLog;
		}

		// ------------------------ 这几个属性不是预定义的，内部处理用
		private String storyName;
		private String logUrl;
		private String repostUrl;
		// 是否被反抓取了
		private volatile boolean anti;
		private volatile boolean etlDone;
		private String antiHtml;
		
		public boolean isEtlDone() {
			return etlDone;
		}
		public void setEtlDone(boolean etlDone) {
			this.etlDone = etlDone;
		}
		public String getAntiHtml() {
			return antiHtml;
		}
		public void setAntiHtml(String antiHtml) {
			this.antiHtml = antiHtml;
		}
		public boolean isAnti() {
			return anti;
		}
		public void setAnti(boolean anti) {
			this.anti = anti;
		}
		public String getRepostUrl() {
			return repostUrl;
		}
		public void setRepostUrl(String repostUrl) {
			this.repostUrl = repostUrl;
		}
		public String getLogUrl() {
			return logUrl;
		}
		public void setLogUrl(String logUrl) {
			this.logUrl = logUrl;
		}
		public String getStoryName() {
			return storyName;
		}
		public void setStoryName(String storyName) {
			this.storyName = storyName;
		}
		
		public static Task deepClone(Task task) {
			
			String taskJson = GSON.toJson(task);
			return GSON.fromJson(taskJson, new TypeToken<Task>() {}.getType());
		}
	}
	
	public static String prepareUrl(String fullPath, String partPath) {
		
		URL url = null;
		try {
			url = new URL(fullPath);
		} catch (MalformedURLException e) {
			// Ignore
			return partPath;
		}
		
		try {
			url = new URL(partPath);
			// 直接返回
			return partPath;
		} catch (MalformedURLException e) {
			// Ignore
		}
		
		if (partPath.startsWith("//")) {
			// Maybe miss protocol
			try {
				String partPartUrl = url.getProtocol() + ":" + partPath;
				new URL(fullPath);
				return partPartUrl;
			} catch (MalformedURLException e) {
				// Ignore
			}
		}
		
		String domain = url.getProtocol() + "://" + url.getHost()
			+ (url.getPort() < 0 ? "" : url.getPort());
		String forReturn = partPath;
		if (!partPath.contains(url.getHost())) {
			forReturn = domain + partPath;
		} else {
			int index = partPath.indexOf(url.getHost());
			forReturn = domain + partPath.substring(index + url.getHost().length());
		}
		
		return forReturn;
	}
	
	public static Map<String, String> parseParams(String url) {
		
		Map<String, String> params = new HashMap<String, String>();
		try {
			URL u = new URL(url);
			String query = u.getQuery();
			if (query != null) {
				String[] pair = query.split("&");
				for (String p : pair) {
					String[] kv = p.split("=");
					if (kv.length > 1) {
						params.put(kv[0], kv[1]);
					}
				}
			}
		} catch (MalformedURLException e) {
			// Ignore
		}
		
		return params;
	}
	
	public static String appendParams(String url, Map<String, String> needAppend) {
		
		try {
			URL u = new URL(url);
			String query = u.getQuery();
			String append = "";
			if (query == null) {
				append += "?"; 
			}
			for (Entry<String, String> entry : needAppend.entrySet()) {
				append += "&" + entry.getKey() + "=" + entry.getValue();
			}
			return url + append;
		} catch (MalformedURLException e) {
			// Ignore
			return url;
		}
	}
	
	public static void main(String[] args) {
		
		String url = prepareUrl("https://www.baidu.com?a=1", "//www.baidu.com/b=2");
		System.out.println(url);
		
		url = prepareUrl("https://www.baidu.com?a=1", "/b=2");
		System.out.println(url);
		
		Map<String, String> p = parseParams("http://www.baidu.com/redirect.html?a=1&b=2");
		System.out.println(p);
		
		Map<String, String> append = new LinkedHashMap<String, String>();
		append.put("1", "2");
		url = appendParams("http://www.baidu.com?a=b", append);
		System.out.println(url);
		
		url = appendParams("http://www.baidu.com", append);
		System.out.println(url);
	}
}
