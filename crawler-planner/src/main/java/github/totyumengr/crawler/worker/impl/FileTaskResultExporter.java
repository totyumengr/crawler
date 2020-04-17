package github.totyumengr.crawler.worker.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import github.totyumengr.crawler.worker.TaskResultExporter;

@Component("file")
public class FileTaskResultExporter implements TaskResultExporter {

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Value("${exporter.basefilepath}")
	private String fileExporterPath;
	
	@Override
	public void export(String fromUrl) {

		logger.info("Blank exporter on url={}", fromUrl);
	}

}
