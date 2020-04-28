package github.totyumengr.crawler.worker.task.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

public class PureTextFileTaskResultExporterTest {

	@Test
	public void testPureTextConvert() {
		
		PureTextFileTaskResultExporter exporter = new PureTextFileTaskResultExporter();
		List<String> c = new ArrayList<String>();
		c.add("=====");
		c.add("http://www.baidu.com");
		c.add("=====");
		c.add("<body>1</body>");
		c.add("<body>\r\n2<span>3</span></body>");
		c.add("<body>2\r\n<span><font>4</font>3</span></body>");
		c.add("<span>3-1</span>");
		c.add("2<span><font>4</font>3</span>");
		
		List<String> pureText = exporter.preWriteToFile(c);
		System.out.println(pureText);
	}
	
	@Test
	public void testPureTextConvertFromFile() throws Exception {
		
		PureTextFileTaskResultExporter exporter = new PureTextFileTaskResultExporter();
		List<String> c = new ArrayList<String>();
		ClassPathResource resource = new ClassPathResource("puretext.data");
		String fileContent = FileUtils.readFileToString(resource.getFile(), "UTF-8");
		c.add(fileContent);
		List<String> pureText = exporter.preWriteToFile(c);
		System.out.println(pureText);
	}
}
