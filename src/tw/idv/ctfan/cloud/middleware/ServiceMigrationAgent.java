package tw.idv.ctfan.cloud.middleware;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import tw.idv.ctfan.cloud.middleware.policy.*;
import tw.idv.ctfan.cloud.middleware.policy.Decision.DispatchDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.MigrationDecision;
import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNodeBase;

import jade.core.AID;
import jade.core.Agent;
import jade.core.behaviours.CyclicBehaviour;
import jade.core.behaviours.OneShotBehaviour;
import jade.core.behaviours.ThreadedBehaviourFactory;
import jade.core.behaviours.TickerBehaviour;
import jade.lang.acl.ACLMessage;
import jade.lang.acl.MessageTemplate;

/*
 * This agent manage all service's migration policy.  It first
 * requests lists of all clusters and all jobs' info.  Then, it will
 * compute and determin wheather or not to migrate service from one
 * cluster to another.
 */

public class ServiceMigrationAgent extends Agent {

	private static final long serialVersionUID = 1L;
	
	ThreadedBehaviourFactory tbf;
	
//	Policy policy = JobCountPolicy.GetPolicy();
//	Policy policy = JobSizePolicy.GetPolicy();
//	Policy policy = ExecutionTimePolicy.GetPolicy();
	Policy policy = JavaPolicy.GetPolicy();
//	Policy policy = JavaPolicy1.GetPolicy();
//	Policy policy = JavaPolicy2.GetPolicy();
	
	
	public void setup()
	{
		super.setup();

		
//		tbf = new ThreadedBehaviourFactory();		
//		
//		
//		this.addBehaviour(tbf.wrap(new TickerBehaviour(this, 3000)
//		{
//			private static final long serialVersionUID = 1L;
//			private ServiceManageBehaviour running = null;
//
//			@Override
//			protected void onTick() {
//				if(running==null || (running!=null&&running.done())) {
//					running = new ServiceManageBehaviour(myAgent);
//					myAgent.addBehaviour(tbf.wrap(running));
//				}				
//			}			
//		}));
//		this.addBehaviour(tbf.wrap(new ListeningBehaviour(this)));
				
		
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
			if(ServiceManageBehaviouronlyInstance == null){
				ServiceManageBehaviouronlyInstance = this;
				
				ACLMessage msg;
				
				
				MigrationDecision decision = null;
				if((decision = policy.GetDecision()) != null)
				{
					msg = new ACLMessage(ACLMessage.INFORM);
					AID aid = new AID("job" + decision.job.UID + "@" + decision.job.currentPosition.address + ":1099/JADE", AID.ISGUID);
					aid.addAddresses("http://" + decision.job.currentPosition.address + "7778/acc");
					msg.addReceiver(aid);
					
					msg.setContent("suggest " + decision.destination.toString());
					myAgent.send(msg);
					
					
					System.out.println(myAgent.getName() + " New Migration Decision");
					
				} else if(policy.GetWaitingJob().size()>0) {
//					System.out.println("===Asking New Job Destination===");
					DispatchDecision dispatchDecision = policy.GetNewJobDestination();
//					System.out.println("===Done Asking New Job Destination===");
					
					
					if(dispatchDecision!=null) {
						ClusterNode dest = dispatchDecision.whereToRun;
						JobNodeBase jn = dispatchDecision.jobToRun;
					
						msg = new ACLMessage(ACLMessage.REQUEST);
						AID recv = new AID(dest.name+"@"+dest.address+":1099/JADE", AID.ISGUID);
						recv.addAddresses("http://"+dest.address+":7778/acc");
						msg.addReceiver(recv);
												
						ByteArrayOutputStream s = new ByteArrayOutputStream();
						String param = jn.transferString();
						
						try {
							s.write(param.getBytes());
							s.write(jn.binaryFile);
						} catch (IOException e) {
							e.printStackTrace();
							return;
						}
						
						//System.out.println(param);
						msg.setByteSequenceContent(s.toByteArray());
						
						myAgent.send(msg);
						
						jn.binaryFile=null;
						
						policy.GetWaitingJob().remove(jn);
						policy.GetRunningJob().add(jn);
						//jn.currentPosition = dest;
					}
				}	
				ServiceManageBehaviouronlyInstance = null;
			}
		}		
	}
	
	private class ListeningBehaviour extends CyclicBehaviour
	{
		private static final long serialVersionUID = 1L;

		public ListeningBehaviour(Agent agent){
			super(agent);
		}

		@Override
		public void action() {
			
			//System.out.println("Test Message");
			
			try {
				ACLMessage msg = myAgent.receive(MessageTemplate.MatchAll());
				
				if(msg == null)	{
					block();
					return;
				}
				
				switch(msg.getPerformative())
				{
				case ACLMessage.CONFIRM:
					break;
				case ACLMessage.REQUEST:
					if(msg.getContent().matches("NewJobRequest"))
						myAgent.addBehaviour(tbf.wrap(new ServiceManageBehaviour(myAgent)));
					break;
				default:
					System.out.println("Got Message");
					System.out.println(msg.getContent());
					break;
				}
				
			} catch(Exception e) {
				e.printStackTrace();
			}			
		}		
	}
}
