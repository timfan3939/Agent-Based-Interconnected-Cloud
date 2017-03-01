package tw.idv.ctfan.cloud.Middleware.policy.Decision;

import tw.idv.ctfan.cloud.Middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.Middleware.policy.data.JobNode;

public class MigrationDecision {
	
	public JobNode job;
	public ClusterNode destination;
	
	public MigrationDecision(JobNode job, ClusterNode destination)
	{
		this.job = job;
		this.destination = destination;
	}

}
