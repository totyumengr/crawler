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

import github.totyumengr.crawler.worker.task.ResultExporter;
import github.totyumengr.crawler.worker.task.TaskWorker.Task;

@Component("file")
public class FileTaskResultExporter extends AbstractResultExporter implements ResultExporter {

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Value("${exporter.basefilepath}")
	private String fileExporterPath;
	
	@Override
	public void doExport(Task task, List<List<String>> extractData) {
		
		File storyFolder = new File(fileExporterPath, task.getStoryName());
		try {
			if (!storyFolder.exists()) {
				FileUtils.forceMkdir(storyFolder);
			}
			// 开始写文件
			File taskFile = new File(storyFolder, convertUrlToFileName(task.getFromUrl()));
			FileUtils.touch(taskFile);
			
			List<String> c = new ArrayList<String>();
			c.add(task.getFromUrl());
			for (List<String> data : extractData) {
				c.addAll(data);
			}
			FileUtils.writeLines(taskFile, c, true);
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
