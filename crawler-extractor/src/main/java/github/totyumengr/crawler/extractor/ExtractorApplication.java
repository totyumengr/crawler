package github.totyumengr.crawler.extractor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * 内容抽取主程序入口。
 * @author mengran7
 *
 */
@SpringBootApplication
@ComponentScan("github.totyumengr.crawler")
public class ExtractorApplication {

	public static void main(String[] args) {
		// 启动
		SpringApplication.run(ExtractorApplication.class, args);
	}

}
