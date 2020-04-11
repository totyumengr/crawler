package github.totyumengr.crawler.fetcher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 抓取主程序入口。
 * @author mengran7
 *
 */
@SpringBootApplication
public class FetcherApplication {

	public static void main(String[] args) {
		// 启动
		SpringApplication.run(FetcherApplication.class, args);
	}

}
