package tw.idv.ctfan.cloud.middleware.policy.Decision;

import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNodeBase;

public class DispatchDecision {
	
	public JobNodeBase jobToRun;
	public ClusterNode whereToRun;
	
	public DispatchDecision(JobNodeBase jobToRun, ClusterNode whereToRun) {
		this.jobToRun = jobToRun;
		this.whereToRun = whereToRun;
	}

}
