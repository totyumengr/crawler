package github.totyumengr.crawler.worker.task;

import github.totyumengr.crawler.worker.task.TaskWorker.Task;

/**
 * 任务结果输出接口
 * @author mengran7
 *
 */
public interface ResultExporter {

	void export(Task task);
}
