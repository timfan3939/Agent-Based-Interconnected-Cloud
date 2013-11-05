package tw.idv.ctfan.cloud.middleware.MPI;

import tw.idv.ctfan.cloud.middleware.Cluster.AdminAgent;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNode;

public class MPIAdminAgent extends AdminAgent {

	@Override
	public String GetJobAgentClassName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean InitilizeCluster() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected String OnEncodeClusterLoadInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String OnEncodeJobInfo(JobNode jn) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String OnEncodeNewJobAgent(JobNode jn) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void OnTerminateCluster() {
		// TODO Auto-generated method stub

	}

}
