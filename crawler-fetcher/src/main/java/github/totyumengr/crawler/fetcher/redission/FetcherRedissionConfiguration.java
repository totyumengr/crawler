package github.totyumengr.crawler.fetcher.redission;

import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.redisson.spring.starter.RedissonAutoConfigurationCustomizer;
import org.springframework.stereotype.Component;

/**
 * 本模块和Redis交互的协议均为String
 * @author mengran7
 *
 */
@Component
public class FetcherRedissionConfiguration implements RedissonAutoConfigurationCustomizer {

	@Override
	public void customize(Config configuration) {
		// 改变为String
		configuration.setCodec(new StringCodec());
	}

}
