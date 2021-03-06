package github.totyumengr.crawler.worker.task.impl;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import github.totyumengr.crawler.Crawlers.Task;
import github.totyumengr.crawler.worker.task.ResultExporter;

@Component("no")
public class BlankTaskResultExporter implements ResultExporter {

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Override
	public void export(Task task, Map<String, String> taskData) {

		logger.info("Blank exporter on url={}", task.getFromUrl());
	}

}
