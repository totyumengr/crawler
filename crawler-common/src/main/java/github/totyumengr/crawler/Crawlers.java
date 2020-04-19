package github.totyumengr.crawler;

import java.net.MalformedURLException;
import java.net.URL;

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
	
	public static final String XPATH_LIST_ELEMENTS = "extractor.paging.list";
	public static final String XPATH_RECORD_ELEMENTS = "extractor.paging.list.record";
	public static final String XPATH_PAGINGBAR_ELEMENTS = "extractor.paging.bar";
	public static final String XPATH_PAGINGBAR_NEXTURL_ELEMENTS = "extractor.paging.bar.nexturl";
	public static final String XPATH_CONTENT = "extractor.content";
	
	public static final String PREFIX_TASK_RELATED_URLS = "worker.task.relatedurls.";
	public static final String STORY_PIPELINE = "worker.pipeline";
	// ---------------------------------------------------
	
	public static final String URL = "url";
	public static final String CONTENT = "content";
	
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
		
		String forReturn = partPath;
		if (!partPath.startsWith(fullPath)) {
			forReturn = url.getProtocol() + "://" + url.getHost()
				+ (url.getPort() < 0 ? "" : url.getPort()) + partPath;
		}
		
		return forReturn;
	}
}
