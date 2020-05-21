package github.totyumengr.crawler.worker.task.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.jd.jss.Credential;
import com.jd.jss.JingdongStorageService;

import github.totyumengr.crawler.Crawlers.Task;
import github.totyumengr.crawler.worker.task.ResultExporter;

@Component("jssfile")
public class JssFileTaskResultExporter extends FileTaskResultExporter implements ResultExporter {

	@Value("${exporter.jss.ak}")
	private String accessKey;
	@Value("${exporter.jss.sk}")
	private String secretKey;
	@Value("${exporter.jss.bucket}")
	private String bucket;
	@Value("${exporter.jss.dir}")
	private String dir;
	
	private JingdongStorageService jss;
	
	@PostConstruct
	private void init() throws Exception {
		
		jss = new JingdongStorageService(new Credential(accessKey, secretKey));
		boolean have = jss.hasBucket(bucket);
		if (!have) {
			throw new Exception("Can not found bucket: " + bucket);
		}
		logger.info("Connected jss with {} and start watching {}", bucket);
	}
	
	@Override
	protected void writeToFile(String storyName, String fileName, List<String> contents, Task task) {
		
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
		    ObjectOutputStream oos = new ObjectOutputStream(bos);
		    oos.writeObject(contents);
		    byte[] bytes = bos.toByteArray();
		    
			InputStream is = new ByteArrayInputStream(bytes);
			
			String md5 = jss.bucket(bucket)
					.object(dir.endsWith("/") ? "" : "/" + storyName + "/" + fileName)
					.entity(bytes.length, is)
					.contentType("text/html")
					.put();
			logger.info("Write {} contents to jss {}", task.getFromUrl(), md5);
		} catch (Exception e) {
			logger.info("Error when try to write to jss {}", task.getFromUrl());
		}
	}
}
