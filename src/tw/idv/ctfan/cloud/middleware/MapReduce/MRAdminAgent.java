package tw.idv.ctfan.cloud.middleware.MapReduce;

import tw.idv.ctfan.cloud.middleware.Cluster.AdminAgent;

public class MRAdminAgent extends AdminAgent {
	private static final long serialVersionUID = -6536487006364985284L;

	public MRAdminAgent() {
		super(new MRJobType());
		// TODO Auto-generated constructor stub
	}

	@Override
	public String GetJobAgentClassName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void OnDecodeNewJob(JobListNode jn, String head, String tail) {
		// TODO Auto-generated method stub

	}

	@Override
	protected String OnEncodeJobInfo(JobListNode jn) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String OnEncodeLoadInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String OnEncodeNewJobAgent(JobListNode jn) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void OnTerminateCluster() {
		// TODO Auto-generated method stub

	}

}
