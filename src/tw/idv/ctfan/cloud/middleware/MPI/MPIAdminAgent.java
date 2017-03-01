package tw.idv.ctfan.cloud.Middleware.MPI;

import tw.idv.ctfan.cloud.Middleware.Cluster.AdminAgent;
import tw.idv.ctfan.cloud.Middleware.policy.data.JobNode;

public class MPIAdminAgent extends AdminAgent {
	private static final long serialVersionUID = -6002055057210057475L;
	
	String hostsValue;

	public MPIAdminAgent() {
		super(new MPIJobType());
	}

	@Override
	public String GetJobAgentClassName() {
		return tw.idv.ctfan.cloud.Middleware.MPI.MPIJobAgent.class.getName();
	}

	@Override
	public boolean InitilizeCluster() {
		return true;
	}

	@Override
	protected String OnEncodeClusterLoadInfo() {
		return (super.m_jobList.size()>0?"Busy":"Free");
	}

	@Override
	protected String OnEncodeJobInfo(JobNode jn) {
		return "Nothing to tell";
	}

	@Override
	public String OnEncodeNewJobAgent(JobNode jn) {
		System.out.println("==");
		jn.DisplayDetailedInfo();
		String result = "";
//			if(jn.GetContinuousAttribute("Thread")>0) {
//				result += jn.GetContinuousAttribute("Thread");
			if(jn.GetDiscreteAttribute("Thread")!=null) {
				// All attribute value are treated as discrete value
				result += jn.GetDiscreteAttribute("Thread");
			} else {
				result += "10";
			}
			
			result += "\t";
			
			if(jn.GetDiscreteAttribute("Command")!=null) {
				result += jn.GetDiscreteAttribute("Command");
			} else {
				result += " ";
			}
			
			result += "\t";
			
			result += hostsValue;
			
		System.out.println("OnEncodeNewJobAgent " + result + "_");
		System.out.println("==");
		return result;
	}

	@Override
	public void OnTerminateCluster() {
		System.out.println("I'm going to be terminated.");
	}

	@Override
	protected boolean OnSetArguments(Object[] args) {
		if(args.length<3) 
			return false;
		
		for(int i=2; i<args.length; i++)
			hostsValue += ( (i!=2?",":"") + (String) args[i] );
		
		return true;
	}

}
