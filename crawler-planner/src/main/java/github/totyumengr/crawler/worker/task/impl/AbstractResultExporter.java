package github.totyumengr.crawler.worker.task.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.gson.reflect.TypeToken;

import github.totyumengr.crawler.Crawlers;
import github.totyumengr.crawler.worker.task.ResultExporter;
import github.totyumengr.crawler.worker.task.TaskWorker.Task;

public abstract class AbstractResultExporter implements ResultExporter {

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	@Autowired
	protected RedissonClient pipelineDataClient;
	
	@SuppressWarnings("unchecked")
	@Override
	public void export(Task task) {

		logger.info("Put result into pipeline {}", task.getFromUrl());
		List<String> allUrl = new ArrayList<String>();
		allUrl.add(task.getFromUrl());
		List<Object> relatedUrls = pipelineDataClient.getList(Crawlers.PREFIX_TASK_RELATED_URLS + task.getFromUrl());
		if (relatedUrls != null) {
			for (Object relatedUrl : relatedUrls) {
				allUrl.add(relatedUrl.toString());
			}
			logger.info("Add related url count {}", relatedUrls.size());
		}
		
		List<List<String>> extractResult = new ArrayList<List<String>>();
		for (String url : allUrl) {
			Object structData = pipelineDataClient.getMap(Crawlers.PREFIX_EXTRACT_DATA + task.getExtractor()).get(url);
			if (structData != null) {
				// 获取结构化抽取内容
				Map<String, Object> extractData = Crawlers.GSON.fromJson(structData.toString(),
						new TypeToken<Map<String, Object>>() {}.getType());
				Object extractContent = extractData.get(Crawlers.EXTRACT_DATA);
				if (extractContent != null) {
					extractResult.addAll((List<List<String>>) extractContent);
				}
			}
		}
		logger.info("Found extracted result data count {}", extractResult.size());
		
		// 做结果内容导出
		doExport(task, extractResult, allUrl);
	}
	
	/**
	 * 
	 * @param task 导出Target对象
	 * @param extractData 任务关联的所有Url的抽取数据，包括{@code Task#getFromUrl()}
	 * @param allUrl 当前任务相关的URL，比如分页的URL
	 */
	public abstract void doExport(Task task, List<List<String>> extractData, List<String> allUrl);

}
