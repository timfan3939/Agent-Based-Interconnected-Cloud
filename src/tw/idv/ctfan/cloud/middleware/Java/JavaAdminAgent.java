package tw.idv.ctfan.cloud.Middleware.Java;

import tw.idv.ctfan.cloud.Middleware.Cluster.*;
import tw.idv.ctfan.cloud.Middleware.policy.data.JobNode;

public class JavaAdminAgent extends AdminAgent {

	public JavaAdminAgent() {
		super(new JavaJobType());
	}

	private static final long serialVersionUID = 1L;

	@Override
	protected String OnEncodeClusterLoadInfo() {
		return (super.m_jobList.size()>0?"Busy":"Free");
	}

	@Override
	public void OnTerminateCluster() {
		System.out.println("I'm going to be terminated.");
		// TODO Auto-generated method stub
	}

	@Override
	public String OnEncodeNewJobAgent(JobNode jn) {
		if(jn.GetDiscreteAttribute("Command")!=null)
			return jn.GetDiscreteAttribute("Command");
		return "";
	}
	
	public String GetJobAgentClassName(){
		return tw.idv.ctfan.cloud.Middleware.Java.JavaJobAgent.class.getName();
	}

	@Override
	public boolean InitilizeCluster() {
		return true;
	}

	@Override
	protected String OnEncodeJobInfo(JobNode jn) {
		return "Nothing to tell";
	}

	@Override
	protected boolean OnSetArguments(Object[] args) {
		return args.length>=2;
	}

}
