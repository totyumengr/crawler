package github.totyumengr.crawler.worker.task.impl;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import github.totyumengr.crawler.Crawlers;
import github.totyumengr.crawler.worker.task.ResultExporter;
import github.totyumengr.crawler.worker.task.TaskWorker.Task;

/**
 * 将抓取结果保存到流水线中，供下游取用。
 * @author mengran7
 *
 */
@Component("pipeline")
public class PipelineResultExporter extends AbstractResultExporter implements ResultExporter {

	@Override
	public void doExport(Task task, List<List<String>> extractData) {
		
		List<String> extractUrl = new ArrayList<String>();
		for (List<String> d : extractData) {
			String url = d.get(0);
			// TODO: 这里需要设计下。除了URL其他的数据怎么办？
			extractUrl.add(url);
		}
		
		// 更新到上下文中
		String json = Crawlers.GSON.toJson(extractUrl);
		pipelineDataClient.getMap(Crawlers.STORY_PIPELINE).put(task.getFromUrl(), json);
		logger.info("Put pipeline of {}", task.getFromUrl());
	}
}
