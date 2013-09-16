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

	@Override
	public void SetJobInfo(JobNode jn) {
		long size = Long.parseLong(jn.GetDiscreteAttribute("Command"));
		jn.AddContinuousAttribute("JobSize", size);
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
	public byte[] EncodeJobNode(JobNode jn, String fileDirectory) {
		// TODO Auto-generated method stub
		return null;
	}

}
