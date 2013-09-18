package tw.idv.ctfan.cloud.middleware.Java;

import jade.core.ContainerID;
import jade.lang.acl.ACLMessage;
import tw.idv.ctfan.cloud.middleware.Cluster.JobType;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNode;

public class JavaJobType extends JobType {

	public JavaJobType() {
		super("Java");
	}

	@Override
	public int DecodeLoadInfo(String line) {
		if(line.compareTo("Free")==0)
			return 0;
		else
			return 100;
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

	@Override
	public void SetJobInfo(JobNode jn) {
		try {
			long size = Long.parseLong(jn.GetDiscreteAttribute("Parameter"));
			jn.AddContinuousAttribute("JobSize", size);
		} catch(Exception e) {
			System.out.println("JavaJobType.SetJobInfo() got some problems");
			e.printStackTrace();
		}
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
	public String EncodeParameter(JobNode jn) {
		return jn.GetDiscreteAttribute("Command");
	}

	@Override
	public String OnDispatchJobMsg(JobNode jn) {
		return ("Parameter:" + jn.GetDiscreteAttribute("Parameter"));
	}

	@Override
	public void UpdateJobNodeInfo(String line, JobNode jn) {
		// TODO Auto-generated method stub
		
	}

}
