package tw.idv.ctfan.cloud.middleware.policy;

public class MigrationDecision {
	
	public HadoopJobNode job;
	public ClusterNode destination;
	
	MigrationDecision(HadoopJobNode job, ClusterNode destination)
	{
		this.job = job;
		this.destination = destination;
	}

}
