package github.totyumengr.crawler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 一些常量定义
 * @author mengran7
 *
 */
public interface Crawlers {

	// Redis Keys ----------------------------------------
	// Queue
	String BACKLOG = "crawler.backlog";
	String RAWDATA = "crawler.rawdata";
	String TASK_DONE = "worker.task.done";

	// Map
	String PROXYPOOL = "crawler.proxypool";
	
	String EXTRACTOR = "crawler.extractor";
	String EXTRACT_DATA_PREFIX = "crawler.structdata.";
	
	String XPATH_LIST_ELEMENTS = "extractor.paging.list";
	String XPATH_RECORD_ELEMENTS = "extractor.paging.list.record";
	String XPATH_PAGINGBAR_ELEMENTS = "extractor.paging.bar";
	String XPATH_PAGINGBAR_NEXTURL_ELEMENTS = "extractor.paging.bar.nexturl";
	String XPATH_CONTENT = "extractor.content";
	
	String TASK_RELATED_URLS = "worker.task.relatedurls";
	// ---------------------------------------------------
	
	String URL = "url";
	String CONTENT = "content";
	String EXTACT_TYPE = "extractor.type";
	String PLEASE_SET_EXTRACT_XPATH = "PLEASE_SET_EXTRACT_XPATH";
	
	Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
}
