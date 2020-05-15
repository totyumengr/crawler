package github.totyumengr.crawler.worker.task;

import java.util.Map;

import github.totyumengr.crawler.Crawlers.Task;

/**
 * 任务结果输出接口
 * @author mengran7
 *
 */
public interface ResultExporter {

	void export(Task task, Map<String, String> taskData);
}
