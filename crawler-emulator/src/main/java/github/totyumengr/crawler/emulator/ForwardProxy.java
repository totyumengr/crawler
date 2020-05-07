package github.totyumengr.crawler.emulator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.monkeywie.proxyee.exception.HttpProxyExceptionHandle;
import com.github.monkeywie.proxyee.intercept.HttpProxyIntercept;
import com.github.monkeywie.proxyee.intercept.HttpProxyInterceptInitializer;
import com.github.monkeywie.proxyee.intercept.HttpProxyInterceptPipeline;
import com.github.monkeywie.proxyee.proxy.ProxyConfig;
import com.github.monkeywie.proxyee.proxy.ProxyType;
import com.github.monkeywie.proxyee.server.HttpProxyServer;
import com.github.monkeywie.proxyee.server.HttpProxyServerConfig;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpRequest;

/**
 * 简单的正向代理服务
 * @author mengran7
 *
 */
public class ForwardProxy {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	private String proxyAuthenticatorName;
	private String proxyAuthenticatorPassword;

	public ForwardProxy(String proxyAuthenticatorName, String proxyAuthenticatorPassword) {
		super();
		this.proxyAuthenticatorName = proxyAuthenticatorName;
		this.proxyAuthenticatorPassword = proxyAuthenticatorPassword;
	}

	public HttpProxyServer buildProxy(String proxyIp, int proxyPort) {
		
		logger.info("Build forward proxy on {} {}", proxyIp, proxyPort);
		
		HttpProxyServerConfig config = new HttpProxyServerConfig();
        ProxyConfig proxyConfig = new ProxyConfig(ProxyType.HTTP, proxyIp, proxyPort, proxyAuthenticatorName, proxyAuthenticatorPassword);
        
        return new HttpProxyServer()
        	.serverConfig(config)
        	.proxyConfig(proxyConfig)
        	.proxyInterceptInitializer(new HttpProxyInterceptInitializer() {
                @Override
                public void init(HttpProxyInterceptPipeline pipeline) {
                    pipeline.addLast(new HttpProxyIntercept() {
                        @Override
                        public void beforeRequest(Channel clientChannel, HttpRequest httpRequest,
                                                  HttpProxyInterceptPipeline pipeline) throws Exception {
                            pipeline.beforeRequest(clientChannel, httpRequest);
                        }
                    });
                }
            })
            .httpProxyExceptionHandle(new HttpProxyExceptionHandle() {
                @Override
                public void beforeCatch(Channel clientChannel, Throwable cause) throws Exception {
                	logger.info("Error occurred", cause);
                }

                @Override
                public void afterCatch(Channel clientChannel, Channel proxyChannel, Throwable cause)
                        throws Exception {
                	logger.info("Error occurred", cause);
                }
            });
	}
}
