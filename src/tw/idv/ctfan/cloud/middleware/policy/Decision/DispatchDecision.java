package tw.idv.ctfan.cloud.middleware.policy.Decision;

import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNode;

public class DispatchDecision {
	
	public JobNode jobToRun;
	public ClusterNode whereToRun;
	
	public DispatchDecision(JobNode jobToRun, ClusterNode whereToRun) {
		this.jobToRun = jobToRun;
		this.whereToRun = whereToRun;
	}

}
