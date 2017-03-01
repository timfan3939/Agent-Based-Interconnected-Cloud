package tw.idv.ctfan.cloud.Middleware.policy.Decision;

import tw.idv.ctfan.cloud.Middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.Middleware.policy.data.JobNode;

public class DispatchDecision {
	
	public JobNode jobToRun;
	public ClusterNode whereToRun;
	
	public DispatchDecision(JobNode jobToRun, ClusterNode whereToRun) {
		this.jobToRun = jobToRun;
		this.whereToRun = whereToRun;
	}

}
