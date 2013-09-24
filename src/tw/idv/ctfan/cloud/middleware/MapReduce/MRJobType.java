package tw.idv.ctfan.cloud.middleware.MapReduce;

import jade.core.ContainerID;
import jade.lang.acl.ACLMessage;
import tw.idv.ctfan.cloud.middleware.Cluster.JobType;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNode;

public class MRJobType extends JobType {

	public MRJobType() {
		super("MapReduce");
	}

	@Override
	public int DecodeLoadInfo(String line) {
		if(line.matches("Free")) 
			return 0;
		else
			return 100;
	}

	@Override
	public String EncodeParameter(JobNode jn) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ContainerID ExtractContainer(ACLMessage msg) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String GetExtension() {
		return ".jar";
	}

	@Override
	public String OnDispatchJobMsg(JobNode jn) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void SetJobInfo(JobNode jn) {
		// TODO Auto-generated method stub

	}

	@Override
	public void UpdateJobNodeInfo(String line, JobNode jn) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean varifyJob(JobNode jn) {
		// TODO Auto-generated method stub
		return false;
	}

}
