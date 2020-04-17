package github.totyumengr.crawler.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 执行计划主程序入口。
 * @author mengran7
 *
 */
@SpringBootApplication
public class WorkerApplication {

	public static void main(String[] args) {
		// 启动
		SpringApplication.run(WorkerApplication.class, args);
	}
}
