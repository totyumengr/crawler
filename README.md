# 介绍
从设计上来说，比较简单，使用Redis来进行任务和调用的一个分布式爬虫。在模块拆分上有：

- Proxypool模块：负责维护代理IP池，定期刷新。
- Fetcher模块：负责从Redis中的Backlog中获取任务，进行实际的抓取。支持IP代理池，抓取失败会退回至池中，直到成功为止。
- Emulator模块：使用Selenium进行抓取动作。
- Extractor模块：负责从Redis中的RawData中拿到原始数据，进行结构化提取。
- Worker模块：负责对抓取任务进行策略制定。在这里会进行一些抓取引擎参数的配置。
- Planner模块：负责Worker Story的制定，补量提交。

## 核心概念之故事（Plan）
计划是由一系列Story组成的，具备Story跟踪、回捞等能力。

## 核心概念之故事（Story）
故事是指一个完整的需求，比如要拿到某瓣上所有的图书Meta信息。故事由多个上下游依赖的任务组成。

```
{
	"name": "抓取豆瓣读书全站的图书信息",
	"tasks": [
		{
			"params": "args",
			"template": "doubanBookTagTask.json"
		},
		{
			"params": "pipeline",
			"template": "doubanBookTask.json"
		}
	],
	"args": [
		"https://book.douban.com/tag/%E5%B0%8F%E8%AF%B4",
		"https://book.douban.com/tag/%E9%9A%8F%E7%AC%94",
		"https://book.douban.com/tag/%E6%97%A5%E6%9C%AC%E6%96%87%E5%AD%A6"
	]
}
```

## 核心概念之任务（Task）
比如要拿到某瓣上的指定标签下的图书信息，可以这么写：

```
{
	"name": "豆瓣读书标签",
	"fromUrl": "https://book.douban.com/tag/%E5%B0%8F%E8%AF%B4",
	"extractor": "paging",
	"extractRules": {
		"extractor.paging.list": "//li[@class='subject-item']",
		"extractor.paging.list.record": "//div[@class='info']/h2/a/@href|//div[@class='info']/h2/a/@title",
		"extractor.paging.bar": "//div[@class='paginator']",
		"extractor.paging.bar.nexturl": "//span[@class='next']/a/@href"
	},
	"pageDown": true,
	"landing": "no",
	"traceLog": "no"
}
```

## 如何构建
1. 下载crawler代码库、下载SeimiCrawler代码库。
2. 准备Docker环境，启动Redis（Single or Cluster）
3. 在crawler目录中，执行build.sh
