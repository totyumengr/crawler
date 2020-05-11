package github.totyumengr.crawler.worker.task.impl;

import java.util.List;

import org.springframework.stereotype.Component;

import github.totyumengr.crawler.worker.task.ResultExporter;

@Component("html")
public class HtmlFileTaskResultExporter extends FileTaskResultExporter implements ResultExporter {

	protected void pushHeader(List<String> c, List<String> allUrl) {
		// Do nothing
	}
	
	protected String convertUrlToFileName(String url) {
		
		String fileName = super.convertUrlToFileName(url);
		return fileName + ".html";
	}
}
