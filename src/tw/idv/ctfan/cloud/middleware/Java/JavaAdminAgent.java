package tw.idv.ctfan.cloud.middleware.Java;

import java.util.ArrayList;

import tw.idv.ctfan.cloud.middleware.Cluster.*;

public class JavaAdminAgent extends AdminAgent {

	public JavaAdminAgent() {
		super(new JavaJobType());
	}

	private static final long serialVersionUID = 1L;

	@Override
	public void OnDecodeNewJob(JobListNode jn, String head, String tail) {
		jn.attributes.put(head, tail);
	}

	@Override
	protected String OnEncodeJobInfo(JobListNode jn) {
		long currentTime = System.currentTimeMillis();
		return "java " + jn.name + " running " + (currentTime-jn.lastExist) + " " + (currentTime-jn.executedTime);
	}

	@Override
	protected String OnEncodeLoadInfo() {
		return (super.m_jobList.size()>0?"Busy":"Free");
	}

	@Override
	public void OnTerminateCluster() {
		System.out.println("I'm going to be terminated.");
	}
}
