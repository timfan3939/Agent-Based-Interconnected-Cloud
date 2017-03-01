package tw.idv.ctfan.cloud.Middleware.MPI;

import jade.core.ContainerID;
import jade.lang.acl.ACLMessage;
import tw.idv.ctfan.cloud.Middleware.Cluster.JobType;
import tw.idv.ctfan.cloud.Middleware.policy.data.JobNode;

/**
 * 
 * @author C.T.Fan
 *
 * Accepted job type: MPI program (Compiled), with number of thread and string type command.<br>
 * The number of thread will be treated as the size of the job.
 * <p>
 * Must have attribute:<br>
 * JobType:MPI<br>
 * Command:<String type command allows you to have user defined parameter<br>
 * Name:<Program name> (Optional)<br>
 * Deadline:<The Deadline of the job> (Optional)<br>
 * BinaryDataLength:<length of the Archive>\n<binaryFileContent><br>
 * Thread:<Number of threads>
 */

public class MPIJobType extends JobType {

	public MPIJobType() {
		super("MPI");
	}	

	@Override
	public String HTTPViewerMessage() {
		return "<h1>MPI Job Notice</h1>" +
				"<p>Accepted job type: MPI program (Compiled), with number of thread and string type command.  The number of thread will be treated as the size of the job.</p>" +
				"<p>Must have attributes</p>" +
				"<ul>" +
				"<li>JobType:MPI</li>" +
				"<li>Command:&ltString type command allows you to have user defined parameter&gt</li>" +
				"<li>Name:&ltProgram name&gt (Optional)</li>" +
				"<li>Deadline:&ltThe Deadline of the job in second&gt (Optional)</li>" +
				"<li>Thread:&ltNumber of threads&gt</li>" +
				"</ul>" +
				"";
	}

	@Override
	public int DecodeClusterLoadInfo(String line) {
		if(line.compareTo("Free")==0)
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
		return ".out";
	}

	@Override
	public void SetJobInfo(JobNode jn) {
		try {
			long size = (jn.GetContinuousAttribute("Thread"));
			if(size>=0)
				jn.AddContinuousAttribute("JobSize", size);
		} catch(Exception e) {
			System.out.println("MPIJobType.SetJobInfo() got some problems");
			jn.AddContinuousAttribute("JobSize", 10);
			jn.AddContinuousAttribute("Thread", 10);
			e.printStackTrace();
		}

	}

	@Override
	public void UpdateJobNodeInfo(String line, JobNode jn) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean varifyJob(JobNode jn) {
		// TODO Auto-generated method stub
		return true;
	}

}
