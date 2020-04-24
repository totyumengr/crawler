package github.totyumengr.crawler.worker.task.impl;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import github.totyumengr.crawler.Crawlers.Task;
import github.totyumengr.crawler.worker.task.ResultExporter;

@Component("file")
public class FileTaskResultExporter extends AbstractResultExporter implements ResultExporter {

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Value("${exporter.story.dir}")
	private String storyExportDir;
	
	private static final String HEADER = "====================";
	
	@Override
	public void doExport(Task task, List<List<String>> extractData, List<String> allUrl) {
		
		File storyFolder = new File(storyExportDir, task.getStoryName());
		try {
			if (!storyFolder.exists()) {
				FileUtils.forceMkdir(storyFolder);
			}
			
			List<String> c = new ArrayList<String>();
			// 输出Header部分
			c.add(HEADER);
			c.addAll(allUrl);
			c.add(HEADER);
			
			boolean needWriteToFile = false;
			// 输出Body部分
			for (List<String> data : extractData) {
				c.addAll(data);
				if (data.size() > 0) {
					needWriteToFile = true;
				}
			}
			
			// 开始写文件
			if (needWriteToFile) {
				File taskFile = new File(storyFolder, convertUrlToFileName(task.getFromUrl()));
				FileUtils.touch(taskFile);
				
				FileUtils.writeLines(taskFile, c, true);	
				logger.info("Done... Write content to file. {}", task.getFromUrl());
			} else {
				logger.info("Do not write to file because empty... {}", task.getFromUrl());
			}
		} catch (IOException e) {
			logger.error("Error when try to export task result url={}", task.getFromUrl());
		}
	}
	
	private String convertUrlToFileName(String url) {
		
		try {
			URL u = new URL(url);
			String path = u.getPath().replace("/", "");
			String query = (u.getQuery() == null ? "" : u.getQuery());
			return path + (query != "" ? "-" + query : "");
		} catch (MalformedURLException e) {
			return UUID.randomUUID().toString();
		}
	}
}
