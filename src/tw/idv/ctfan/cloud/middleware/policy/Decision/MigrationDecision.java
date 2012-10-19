package tw.idv.ctfan.cloud.middleware.policy.Decision;

import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.HadoopJobNode;

public class MigrationDecision {
	
	public HadoopJobNode job;
	public ClusterNode destination;
	
	public MigrationDecision(HadoopJobNode job, ClusterNode destination)
	{
		this.job = job;
		this.destination = destination;
	}

}
