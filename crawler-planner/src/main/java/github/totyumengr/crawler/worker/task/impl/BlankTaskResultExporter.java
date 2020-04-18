package github.totyumengr.crawler.worker.task.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import github.totyumengr.crawler.worker.task.ResultExporter;
import github.totyumengr.crawler.worker.task.TaskWorker.Task;

@Component("no")
public class BlankTaskResultExporter implements ResultExporter {

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Override
	public void export(Task task) {

		logger.info("Blank exporter on url={}", task.getFromUrl());
	}

}
