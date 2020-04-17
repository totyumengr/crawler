package github.totyumengr.crawler.extractor;

import java.net.URL;

import org.junit.Assert;
import org.junit.Test;

public class NextPageTest {

	@Test
	public void test() throws Exception {
		
		URL url = new URL("https://book.douban.com/tag/%E5%B0%8F%E8%AF%B4");
		
		Assert.assertEquals("https", url.getProtocol());
		Assert.assertEquals("book.douban.com", url.getHost());
		Assert.assertEquals(-1, url.getPort());
	}

}
