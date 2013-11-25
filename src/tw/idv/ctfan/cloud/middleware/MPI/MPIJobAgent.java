package tw.idv.ctfan.cloud.middleware.MPI;

import jade.core.Agent;
import tw.idv.ctfan.cloud.middleware.Cluster.JobAgent;

public class MPIJobAgent extends JobAgent {
	private static final long serialVersionUID = -3787070421334031975L;
	
	private String hosts = "hdp207,hdp208";

	@Override
	protected String OnHeartBeat() {
		return "Hello, There";
	}

	@Override
	protected void StartJob(Agent myAgent, String info) {
		String[] cmd = info.split("\t");
		if(cmd.length!=2) {
			WriteLog("Info size is not 2");
			WriteLog(info);
			return;
		}
		String command = "mpiexec --host " + hosts + " -n " + cmd[0] + GetBinaryFullPath() + cmd[1];
		WriteLog(command);
		
		// TODO Auto-generated method stub

	}

}
