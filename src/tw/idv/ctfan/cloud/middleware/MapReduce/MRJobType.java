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
	public int DecodeClusterLoadInfo(String line) {
		if(line.matches("Free")) 
			return 0;
		else
			return 100;
	}

	@Override
	public ContainerID ExtractContainer(ACLMessage msg) {
		String content = msg.getContent();
		String[] line = content.split("\n");
		ContainerID cid = new ContainerID();
		cid.setName(line[2]);
		return cid;
	}

	@Override
	public String GetExtension() {
		return ".jar";
	}

	@Override
	public String OnDispatchJobMsg(JobNode jn) {
		return ("Command:" + jn.GetDiscreteAttribute("Command"));
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
		if(jn.GetDiscreteAttribute("Command")==null) return false;
		if(jn.GetDiscreteAttribute("InputFolder")==null) return false;
		if(jn.GetDiscreteAttribute("OutputFolder")==null) return false;
		return true;
	}

}
