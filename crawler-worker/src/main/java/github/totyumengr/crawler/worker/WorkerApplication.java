package github.totyumengr.crawler.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * 执行计划主程序入口。
 * @author mengran7
 *
 */
@SpringBootApplication
@ComponentScan("github.totyumengr.crawler")
public class WorkerApplication {

	public static void main(String[] args) {
		// 启动
		SpringApplication.run(WorkerApplication.class, args);
	}
}
