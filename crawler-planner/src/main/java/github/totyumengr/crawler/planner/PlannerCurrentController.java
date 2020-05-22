package github.totyumengr.crawler.planner;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value="/plans/current")
public class PlannerCurrentController {
	
	@Autowired
	private SavePointPlanner planner;
	
	@RequestMapping(value="/", method=RequestMethod.GET)
	@ResponseBody
	public Object getPlans() {
		
		Map<String, Object> result = new LinkedHashMap<String, Object>();
		List<String> plans = planner.getRunningPlan();
		for (String planName : plans) {
			boolean hasDone = planner.planHasDone(planName);
			result.put(planName, hasDone);
		}
		return result;
	}
	
	@RequestMapping(value="/{planName}/storys/left", method=RequestMethod.GET)
	@ResponseBody
	public Object getLeftStorysOfPlan(@PathVariable String planName) {
		
		Map<String, Object> result = new LinkedHashMap<String, Object>();
		List<String> storys = planner.getStorysOfPlan(planName);
		result.put("count", storys.size());
		result.put("data", storys);
		return result;
	}
	
	@RequestMapping(value="/{planName}/storys/doing", method=RequestMethod.GET)
	@ResponseBody
	public Object getDoingStorysOfPlan(@PathVariable String planName) {
		
		Map<String, Object> result = new LinkedHashMap<String, Object>();
		List<String> runningStorys = planner.getRunningStorysOfPlan(planName);
		result.put("count", runningStorys.size());
		result.put("data", runningStorys);
		return result;
	}
	
	@RequestMapping(value="/{planName}/storys/doing/statistics", method=RequestMethod.GET)
	@ResponseBody
	public Object getDoingStorysStatisticsOfPlan(@PathVariable String planName) {
		
		Map<String, Object> result = new LinkedHashMap<String, Object>();
		List<String> runningStorys = planner.getRunningStorysOfPlan(planName);
		result.put("count", runningStorys.size());
		result.put("data", runningStorys);
		return result;
	}
	
	@RequestMapping(value="/{planName}/storys/doing/detail", method=RequestMethod.GET)
	@ResponseBody
	public Object getDoingStorysDetailOfPlan(@PathVariable String planName) {
		
		Map<String, Object> result = new LinkedHashMap<String, Object>();
		List<String> runningStorys = planner.getRunningStorysOfPlan(planName);
		for (String storyName : runningStorys) {
			Map<String, List<Pair<String, String>>> tasks = planner.getTasksOfStory(storyName);
			result.put(storyName, tasks);
		}
		return result;
	}
}
