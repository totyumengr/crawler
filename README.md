# 介绍
从设计上来说，比较简单，使用Redis来进行任务和调用的一个分布式爬虫。在模块拆分上有：

- Fetcher模块：负责从Redis中的Backlog中获取任务，进行实际的抓取。支持IP代理池，抓取失败会退回至池中，直到成功为止。
- Extractor模块：负责从Redis中的RawData中拿到原始数据，进行结构化提取。

## 举例
比如要拿到某瓣上的图书，其流程是：
 - hset crawler.extractor https://book.douban.com/tag/%E5%B0%8F%E8%AF%B4 paging
 - hset extractor.paging.list https://book.douban.com/tag/%E5%B0%8F%E8%AF%B4 "//li[@class='subject-item']"
 - hset extractor.paging.list.record https://book.douban.com/tag/%E5%B0%8F%E8%AF%B4 "//div[@class='info']/h2/a/@href|//div[@class='info']/h2/a/@title"
 - hset extractor.paging.bar https://book.douban.com/tag/%E5%B0%8F%E8%AF%B4 "//div[@class='paginator']"
 - hset extractor.paging.bar.nexturl https://book.douban.com/tag/%E5%B0%8F%E8%AF%B4 "//span[@class='next']/a/@href"
 - rpush backlog https://book.douban.com/tag/%E5%B0%8F%E8%AF%B4

然后，就可以该URL提取的结构化数据就放到structData数据结构中。

# TODO
- Planner模块：负责对抓取任务进行策略制定。在这里会进行一些抓取引擎参数的配置。
