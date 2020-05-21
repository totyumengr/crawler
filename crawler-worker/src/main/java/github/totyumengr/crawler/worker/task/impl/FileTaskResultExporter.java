package github.totyumengr.crawler.worker.task.impl;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import github.totyumengr.crawler.Crawlers;
import github.totyumengr.crawler.Crawlers.Task;
import github.totyumengr.crawler.Crawlers.Task.STATUS;
import github.totyumengr.crawler.worker.task.ResultExporter;

@Component("file")
public class FileTaskResultExporter extends AbstractResultExporter implements ResultExporter {

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Value("${exporter.story.dir}")
	private String storyExportDir;
	@Autowired
	private RedissonClient pipelineDataClient;
	
	@Override
	public void doExport(Task task, List<List<String>> extractData, Collection<String> allUrl) {
		
		List<String> c = new ArrayList<String>();
		pushHeader(c, allUrl);
		
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
			String fileName = convertUrlToFileName(task.getFromUrl());
			List<String> writeTo = preWriteToFile(c);
			writeToFile(task.getStoryName(), fileName, writeTo, task);
			task.setStatus(STATUS.EXPORTED.name());
		} else {
			task.setStatus(STATUS.NO_EXPORTED.name());
			logger.info("Do not write to file because empty... {}", task.getFromUrl());
		}
		
		// 记录Trace
		if (task.isTraceLog()) {
			pipelineDataClient.getListMultimap(task.getStoryName() + Crawlers.STORY_TRACE).get(task.getFromUrl())
				.add(Crawlers.GSON.toJson(task));
		}
	}
	
	private static final String HEADER = "====================";
	
	protected void pushHeader(List<String> c, Collection<String> allUrl) {
		
		// 输出Header部分
		c.add(HEADER);
		c.addAll(allUrl);
		c.add(HEADER);
	}
	
	protected void writeToFile(String storyName, String fileName, List<String> contents, Task task) {
		
		try {
			File storyFolder = new File(storyExportDir, storyName);
			if (!storyFolder.exists()) {
				FileUtils.forceMkdir(storyFolder);
			}
			File taskFile = new File(storyFolder, fileName);
			if (taskFile.exists()) {
				FileUtils.forceDelete(taskFile);
			}
			FileUtils.touch(taskFile);
			
			FileUtils.writeLines(taskFile, contents, true);	
			logger.info("Done... Write content to file. {}", task.getFromUrl());
		} catch (Exception e) {
			logger.error("Error when try to export task result url={}", task.getFromUrl(), e);
		}
	}
	
	protected List<String> preWriteToFile(List<String> contents) {
		// Do nothing
		return contents;
	}
	
	protected String convertUrlToFileName(String url) {
		
		try {
			URL u = new URL(url);
			String path = u.getPath().replace("/", "");
			String query = (u.getQuery() == null ? "" : u.getQuery());
			String fileName = path + (query != "" ? "-" + query : "");
			return fileName.length() > 80 ? fileName.substring(0, 80) : fileName;
		} catch (MalformedURLException e) {
			return UUID.randomUUID().toString();
		}
	}
}
