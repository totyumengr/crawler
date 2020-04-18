package github.totyumengr.crawler.worker.task.impl;

import java.util.List;

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
		
		
	}
}
