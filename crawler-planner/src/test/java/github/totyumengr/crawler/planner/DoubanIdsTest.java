package github.totyumengr.crawler.planner;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringRunner;

import github.totyumengr.crawler.planner.impl.FromFilePlanner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes= {PlannerApplication.class})
@Import(Config.class)
public class DoubanIdsTest {
	
	@Autowired
	private FromFilePlanner fromFilePlanner;
	
	@Test
	public void testRun() throws Exception {
		
		fromFilePlanner.END.await();
		Assert.assertTrue("Done...", true);
	}
}
