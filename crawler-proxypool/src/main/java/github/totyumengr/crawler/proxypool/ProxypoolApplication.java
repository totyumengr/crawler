package github.totyumengr.crawler.proxypool;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * 抓取主程序入口。
 * @author mengran7
 *
 */
@SpringBootApplication
@ComponentScan("github.totyumengr.crawler")
public class ProxypoolApplication {

	public static void main(String[] args) {
		// 启动
		SpringApplication.run(ProxypoolApplication.class, args);
	}

}
