package github.totyumengr.crawler.extractor;

import java.net.URL;

import org.junit.Assert;
import org.junit.Test;
import org.seimicrawler.xpath.JXDocument;

import github.totyumengr.crawler.Crawlers;

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
	
	@Test
	public void test302() throws Exception {
		
		URL url1 = new URL("https://search.smzdm.com/?c=home&s=送妈妈&order=time&v=b");
		URL url2 = new URL("https://search.smzdm.com/?c=home&s=%E9%80%81%E5%A6%88%E5%A6%88&order=time&v=b");
		Assert.assertEquals(url1.getHost(), url2.getHost());
		Assert.assertEquals(url1.getPath(), url2.getPath());
		url1 = new URL("https://search.smzdm.com");
		url2 = new URL("https://search.smzdm.com");
		Assert.assertEquals(true, url1.getPath() == url2.getPath());
		Assert.assertEquals(true, url1.getPath().equals(url2.getPath()));
	}
	
	@Test
	public void testPrepareUrl() {
		
		String partPath = "//post.smzdm.com/p/aqnl5x3k/";
		String fullPath = "https://search.smzdm.com/?c=post&s=送妈妈&order=time&v=b";
		
		String url = Crawlers.prepareUrl(fullPath, partPath);
		Assert.assertEquals("https:" + partPath, url);
	}
	
	@Test
	public void testDocument() {
		JXDocument.create("<html><head>NO_CONTENT</head><body>NO_CONTENT</body></html>");
	}

}
