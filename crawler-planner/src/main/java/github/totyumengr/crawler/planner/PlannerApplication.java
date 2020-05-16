package github.totyumengr.crawler.planner;

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
public class PlannerApplication {

	public static void main(String[] args) {
		// 启动
		SpringApplication.run(PlannerApplication.class, args);
	}
}
