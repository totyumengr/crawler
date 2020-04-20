package github.totyumengr.crawler.extractor;

import java.net.URL;

import org.junit.Assert;
import org.junit.Test;

public class NextPageTest {

	@Test
	public void test() throws Exception {
		
		URL url = new URL("https://book.douban.com/tag/%E5%B0%8F%E8%AF%B4?abc=133&456");
		
		Assert.assertEquals("https", url.getProtocol());
		Assert.assertEquals("book.douban.com", url.getHost());
		Assert.assertEquals(-1, url.getPort());
		
		Assert.assertEquals("/tag/%E5%B0%8F%E8%AF%B4", url.getPath());
		Assert.assertEquals("abc=133&456", url.getQuery());
		
		URL url1 = new URL("https://book.douban.com?abc=133&456");
		Assert.assertEquals("", url1.getPath());
		Assert.assertEquals("abc=133&456", url1.getQuery());
		
		URL url2 = new URL("https://book.douban.com");
		Assert.assertEquals("", url2.getPath());
		Assert.assertEquals(null, url2.getQuery());
	}
	
	@Test
	public void testUrl() throws Exception {
		
		URL url = new URL("http://www.baidu.com/redirect.html?abc=23r&dsfsf=3r2432");
		Assert.assertEquals("abc=23r&dsfsf=3r2432", url.getQuery());
	}

}
