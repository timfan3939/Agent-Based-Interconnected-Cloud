package tw.idv.ctfan.cloud.middleware;

import tw.idv.ctfan.cloud.middleware.policy.*;
import tw.idv.ctfan.cloud.middleware.policy.Decision.DispatchDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.MigrationDecision;
import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNode;
import jade.core.Agent;
import jade.core.behaviours.OneShotBehaviour;
import jade.core.behaviours.ThreadedBehaviourFactory;
import jade.core.behaviours.TickerBehaviour;
import jade.lang.acl.ACLMessage;

/*
 * This agent manage all service's migration policy.  It first
 * requests lists of all clusters and all jobs' info.  Then, it will
 * compute and determine whether or not to migrate service from one
 * cluster to another.
 */

public class JobDispatchingAgent extends Agent {
	
	private static final long serialVersionUID = 6274680676517501589L;
	
	private final String fileDirectory = "C:\\ctfan\\middlewareFile\\";

	ThreadedBehaviourFactory tbf;
	
	Policy policy = MultiTypePolicy.GetPolicy();
	
	
	public void setup()
	{
		super.setup();

		
		tbf = new ThreadedBehaviourFactory();		
		
		
		this.addBehaviour(tbf.wrap(new TickerBehaviour(this, 3000)
		{
			private static final long serialVersionUID = 1L;
			private ServiceManageBehaviour running = null;

			@Override
			protected void onTick() {
				if(running==null || (running!=null&&running.done())) {
					running = new ServiceManageBehaviour(myAgent);
					myAgent.addBehaviour(tbf.wrap(running));
				}				
			}			
		}));
				
		
	}	
	public ServiceManageBehaviour ServiceManageBehaviouronlyInstance = null;
	
	private class ServiceManageBehaviour extends OneShotBehaviour
	{
		private static final long serialVersionUID = 1L;

		public ServiceManageBehaviour(Agent a)
		{
			super(a);
		}

		@Override
		public void action() {
			synchronized(policy) {
				if(ServiceManageBehaviouronlyInstance == null){
					ServiceManageBehaviouronlyInstance = this;
					
					ACLMessage msg;
					
					
					MigrationDecision decision = null;
					if((decision = policy.GetMigrationDecision()) != null)
					{
						// TODO: What to do when migration
					} else if(policy.GetWaitingJob().size()>0) {
	//					System.out.println("===Asking New Job Destination===");
						DispatchDecision dispatchDecision = policy.GetNewJobDestination();
	//					System.out.println("===Done Asking New Job Destination===");
						
						
						if(dispatchDecision!=null) {
							ClusterNode dest = dispatchDecision.whereToRun;
							JobNode jn = dispatchDecision.jobToRun;
						
							msg = new ACLMessage(ACLMessage.REQUEST);
							msg.addReceiver(dest.agentID);

							msg.setByteSequenceContent(jn.jobType.EncodeJobNode(jn, fileDirectory+jn.UID+jn.jobType.GetExtension()));
							
							myAgent.send(msg);							
							
							policy.GetWaitingJob().remove(jn);
							policy.GetRunningJob().add(jn);
						}
					}	
					ServiceManageBehaviouronlyInstance = null;
				}
			}		
		}
	}
}
