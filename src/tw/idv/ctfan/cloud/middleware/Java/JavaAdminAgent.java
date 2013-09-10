package tw.idv.ctfan.cloud.middleware.Java;

import tw.idv.ctfan.cloud.middleware.Cluster.*;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNode;

public class JavaAdminAgent extends AdminAgent {

	private static final long serialVersionUID = 1L;

	@Override
	public JobListNode OnDecodeNewJob(byte[] data) {
		// TODO Auto-generated method stub
		return null;
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
	public boolean CheckJobType(JobNode job) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String GetClusterType() {
		// TODO Auto-generated method stub
		return null;
	}

}
