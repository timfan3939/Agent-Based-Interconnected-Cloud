package tw.idv.ctfan.cloud.Middleware.Workflow;

import jade.core.ContainerID;
import jade.lang.acl.ACLMessage;
import tw.idv.ctfan.cloud.Middleware.Cluster.JobType;
import tw.idv.ctfan.cloud.Middleware.policy.data.JobNode;

public class TaskType extends JobType{
	
	public TaskType(){
		super("Java");
	}

	@Override
	public String HTTPViewerMessage() {
		return "<h1>Workflow Task Notice</h1>" +
				"<p>Accepted Task type: workflow runnable archive, with only one Long type command.  The long type command will be treated as the size of the task.</p>" +
				"<p>Must have attributes</p>" +
				"<ul>" +
				"<li>JobType:Workflow Task</li>" +
				"<li>Command:&ltThe Long type command to execute the task&gt</li>" +
				"<li>Name:&ltProgram name&gt (Optional)</li>" +
				"<li>Deadline:&ltThe Deadline of the job in second&gt (Optional)</li>" +
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
	public void UpdateJobNodeInfo(String line, JobNode jn) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean varifyJob(JobNode jn) {
		// TODO Auto-generated method stub
		return true;
	}


	@Override
	public String GetExtension() {
		// TODO Auto-generated method stub
		return ".zip";
	}
	
	@Override
	public void SetJobInfo(JobNode jn) {
		try {
			long size = Long.parseLong(jn.GetDiscreteAttribute("Command"));
			if(size>=0)
				jn.AddContinuousAttribute("JobSize", size);
		} catch(Exception e) {
			System.out.println("WorkflowJobType.SetJobInfo() got some problems");
			jn.AddContinuousAttribute("JobSize", 10);
			jn.AddContinuousAttribute("Thread", 10);
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

}
