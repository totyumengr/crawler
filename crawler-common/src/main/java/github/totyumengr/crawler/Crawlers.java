package github.totyumengr.crawler;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 一些常量定义
 * @author mengran7
 *
 */
public final class Crawlers {

	// Redis Keys ----------------------------------------
	// Queue
	public static final String BACKLOG = "crawler.backlog";
	public static final String RAWDATA = "crawler.rawdata";

	// Map
	public static final String PROXYPOOL = "crawler.proxypool";
	
	public static final String EXTRACTOR = "crawler.extractor";
	public static final String PREFIX_EXTRACT_DATA = "crawler.structdata.";
	public static final String COOKIES = "fetcher.cookies";
	
	public static final String XPATH_LIST_ELEMENTS = "extractor.paging.list";
	public static final String XPATH_RECORD_ELEMENTS = "extractor.paging.list.record";
	public static final String XPATH_PAGINGBAR_ELEMENTS = "extractor.paging.bar";
	public static final String XPATH_PAGINGBAR_NEXTURL_ELEMENTS = "extractor.paging.bar.nexturl";
	public static final String XPATH_CONTENT = "extractor.content";
	
	public static final String PREFIX_TASK_RELATED_URLS = "worker.task.relatedurls.";
	public static final String STORY_PIPELINE = "worker.pipeline";
	
	// Trace
	public static final String PREFIX_STORY_TRACE = "worker.trace.";
	// ---------------------------------------------------
	
	public static final String URL = "url";
	public static final String CONTENT = "content";
	public static final String REPOST = "repost";
	public static final String REPOST_COOKIE = "repost.cookie";
	
	public static final String TASK_TEMPLATE = "template";
	public static final String TASK_PARAMS = "params";
	public static final String TASK_PARAMS_ARGS = "args";
	public static final String TASK_PARAMS_PIPELINE = "pipeline";
	
	public static final String EXTRACT_DATA = "structdata";
	public static final String PLEASE_SET_EXTRACT_XPATH = "PLEASE_SET_EXTRACT_XPATH";
	
	public static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
	
	public static String prepareUrl(String fullPath, String partPath) {
		
		URL url = null;
		try {
			url = new URL(fullPath);
		} catch (MalformedURLException e) {
			// Ignore
			return partPath;
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
					params.put(kv[0], kv[1]);
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
