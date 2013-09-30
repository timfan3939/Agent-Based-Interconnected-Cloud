package tw.idv.ctfan.cloud.middleware.Java;

import jade.core.ContainerID;
import jade.lang.acl.ACLMessage;
import tw.idv.ctfan.cloud.middleware.Cluster.JobType;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNode;

/**
 * 
 * @author C.T.Fan
 * 
 * Accepted job type: java runnable archive, with only one Long type command.<br>
 * The long type command will be treated as the size of the job.
 * <p>
 * Must have attribute:<br>
 * JobType:Java<br>
 * Command:<The Long type command to execute the job><br>
 * Name:<Program name>  (Optional)<br>
 * Deadline:<The Deadline of the job>  (Optional)<br>
 * BinaryDataLength:<length of the Archive>\n<binaryFileContent><br>
 *
 */

public class JavaJobType extends JobType {

	public JavaJobType() {
		super("Java");
	}

	@Override
	public int DecodeClusterLoadInfo(String line) {
		if(line.compareTo("Free")==0)
			return 0;
		else
			return 100;
	}

	@Override
	public boolean varifyJob(JobNode jn) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public String GetExtension() {
		return ".jar";
	}

	@Override
	public void SetJobInfo(JobNode jn) {
		try {
			long size = Long.parseLong(jn.GetDiscreteAttribute("Command"));
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
	public String OnDispatchJobMsg(JobNode jn) {
		return ("Command:" + jn.GetDiscreteAttribute("Command"));
	}

	@Override
	public void UpdateJobNodeInfo(String line, JobNode jn) {
		// TODO Auto-generated method stub
		
	}

}
