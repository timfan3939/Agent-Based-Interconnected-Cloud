package tw.idv.ctfan.cloud.middleware.Java;

import tw.idv.ctfan.cloud.middleware.Cluster.JobType;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNode;

public class JavaJobType extends JobType {

	public JavaJobType() {
		super("Java");
	}

	@Override
	public int DecodeLoadInfo() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean varifyJob(JobNode jn) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String GetExtension() {
		return ".jar";
	}

}
