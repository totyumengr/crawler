package github.totyumengr.crawler.worker.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import github.totyumengr.crawler.worker.TaskResultExporter;

@Component("no")
public class BlankTaskResultExporter implements TaskResultExporter {

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Override
	public void export(String fromUrl) {

		logger.info("Blank exporter on url={}", fromUrl);
	}

}
