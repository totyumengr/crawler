package github.totyumengr.crawler.worker;

/**
 * 任务结果输出接口
 * @author mengran7
 *
 */
public interface TaskResultExporter {

	void export(String fromUrl);
}
