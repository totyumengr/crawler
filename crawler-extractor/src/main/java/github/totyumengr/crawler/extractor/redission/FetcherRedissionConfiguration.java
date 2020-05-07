package github.totyumengr.crawler.extractor.redission;

import java.util.HashMap;
import java.util.Map;

import org.redisson.api.HostPortNatMapper;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.redisson.spring.starter.RedissonAutoConfigurationCustomizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 本模块和Redis交互的协议均为String
 * @author mengran7
 *
 */
@Component
public class FetcherRedissionConfiguration implements RedissonAutoConfigurationCustomizer {

	@Value("${mac.docker.env}")
	private boolean macDockerEnv;
	
	@Override
	public void customize(Config configuration) {
		// 改变为String
		configuration.setCodec(new StringCodec());
		
		if (macDockerEnv) {
			HostPortNatMapper netMapper = new HostPortNatMapper();
			Map<String, String> hostPort = new HashMap<String, String>();
			hostPort.put("127.0.0.1:7000", "host.docker.internal:7000");
			hostPort.put("127.0.0.1:7001", "host.docker.internal:7001");
			hostPort.put("127.0.0.1:7002", "host.docker.internal:7002");
			hostPort.put("127.0.0.1:7003", "host.docker.internal:7003");
			hostPort.put("127.0.0.1:7004", "host.docker.internal:7004");
			hostPort.put("127.0.0.1:7005", "host.docker.internal:7005");
			hostPort.put("127.0.0.1:7006", "host.docker.internal:7006");
			hostPort.put("127.0.0.1:7007", "host.docker.internal:7007");
			hostPort.put("127.0.0.1:7008", "host.docker.internal:7008");
			hostPort.put("127.0.0.1:7009", "host.docker.internal:7009");
			
			netMapper.setHostsPortMap(hostPort);
			configuration.useClusterServers().setNatMapper(netMapper);
		}
	}
}
