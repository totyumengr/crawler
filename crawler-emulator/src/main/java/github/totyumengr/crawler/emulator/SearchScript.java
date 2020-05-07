package github.totyumengr.crawler.emulator;

import java.util.Map;

import github.totyumengr.crawler.Crawlers;
import github.totyumengr.crawler.Crawlers.Task;

class SearchScript {
	
	private String url;
	private String keyword;

	private String keyWordElementId;
	private String searchResultContainerElementId;
	private String recordXpath;
	private int pageDownCount;
	private String nextPageXpath;
	private String searchToolXpath;
	private String timeRangeXpath;
	
	public String getSearchToolXpath() {
		return searchToolXpath;
	}
	public void setSearchToolXpath(String searchToolXpath) {
		this.searchToolXpath = searchToolXpath;
	}
	public String getTimeRangeXpath() {
		return timeRangeXpath;
	}
	public void setTimeRangeXpath(String timeRangeXpath) {
		this.timeRangeXpath = timeRangeXpath;
	}
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
//		ss.setUrl(task.getFromUrl().replaceAll(Crawlers.SEARCH_KEYWORD + "=" + ss.getKeyword(), ""));
		ss.setUrl(task.getFromUrl());
		
		return ss;
	}
}
