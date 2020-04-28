package github.totyumengr.crawler.worker.task.impl;

import java.util.ArrayList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.safety.Whitelist;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import github.totyumengr.crawler.worker.task.ResultExporter;

@Component("puretextfile")
public class PureTextFileTaskResultExporter extends FileTaskResultExporter implements ResultExporter {

	protected Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	protected List<String> preWriteToFile(List<String> contents) {
		
		// 将内容处理成纯文本，去除其他的标签。
		if (contents == null || contents.size() == 0) {
			return contents;
		}
		List<String> pureText = new ArrayList<String>(contents.size());
		for (String content : contents) {
			String text;
			if (!content.startsWith("<body>")) {
				text = "<body>" + content + "</body>";
			} else {
				text = content;
			}
			text = Jsoup.clean(text, "", Whitelist.none(), new Document.OutputSettings().prettyPrint(false));
			pureText.add(text);
		}
		return pureText;
	}
}
