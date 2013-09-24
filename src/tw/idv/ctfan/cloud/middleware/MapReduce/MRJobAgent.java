package tw.idv.ctfan.cloud.middleware.MapReduce;

import jade.core.Agent;
import tw.idv.ctfan.cloud.middleware.Cluster.JobAgent;

public class MRJobAgent extends JobAgent {
	private static final long serialVersionUID = 3217299124951417949L;

	@Override
	protected String OnHeartBeat() {
		return "Hello, There.";
	}

	@Override
	protected void StartJob(Agent myAgent, String info) {
		// TODO Auto-generated method stub

	}

}
