package tw.idv.ctfan.cloud.middleware;

import java.util.Map;

import com.xensource.xenapi.Types;
import com.xensource.xenapi.VM;

import tw.idv.ctfan.cloud.middleware.Cluster.JobType;
import tw.idv.ctfan.cloud.middleware.policy.MultiTypePolicy;
import tw.idv.ctfan.cloud.middleware.policy.Policy;
import tw.idv.ctfan.cloud.middleware.policy.Decision.VMManagementDecision;
import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.VMController;
import jade.core.Agent;
import jade.core.behaviours.OneShotBehaviour;
import jade.core.behaviours.ThreadedBehaviourFactory;
import jade.core.behaviours.TickerBehaviour;
import jade.lang.acl.ACLMessage;

/*
 * This agent check if any cluster should shut down or boot up
 * This agent check if any agent (job) should be migrate to other cluster (but
 * not specify any agent).
 * The agent periodically ask AdministratorAgent to obtain the information of
 * all cluster and agent.  Then make decision.  If need to change, send INFORM
 * to ServiceMigrationAgent to migrate.
 */

public class ResourceReconfigurationAgent extends Agent {
	private static final long serialVersionUID = -2691024839013212067L;

	Policy policy = MultiTypePolicy.GetPolicy();
	
	public static final String name = "RRA";
		
	private ThreadedBehaviourFactory tbf;
	
	private enum STATE {
		Normal, Starting_VM, Closing_VM
	};
	private STATE state;
	private ClusterNode currentCluster;
	
	public void setup() 
	{
		super.setup();
		
		synchronized(policy) {
			state = STATE.Normal;
			currentCluster = null;
		
			policy.InitVMMasterList();
			policy.InitClusterList();
			
			// Close every VM
			InitAllVM();
			
			// Start up one cluster for each type of job
			for(JobType jt:policy.GetJobTypeList()) {
				for(ClusterNode cn : policy.GetAvailableCluster()) {
					if(cn.jobType == jt) {
						try {
							cn.StartCluster();
						} catch(Exception e) {
							System.out.println("Open cluster problem");
							e.printStackTrace();
						}
						policy.GetAvailableCluster().remove(cn);
						policy.GetRunningCluster().add(cn);
						break;
					}
				}
			}
		}
		
		tbf = new ThreadedBehaviourFactory();
		this.addBehaviour(tbf.wrap(new TickerBehaviour(this, 3000){
			private static final long serialVersionUID = 1L;
			private VMManageBehaviour running = null;
			@Override
			protected void onTick() {
				if((running!=null&&running.done()) || running==null){
					myAgent.addBehaviour(tbf.wrap(running=new VMManageBehaviour(myAgent)));
				}
			}			
		}));
		
	}
	
	private void InitAllVM() {
		try {
			for(VMController vmc:policy.GetVMControllerList()) {
				Map<VM, VM.Record> VMs = VM.getAllRecords(vmc.xenConnection);
				
				for(VM.Record record:VMs.values()) {
					VM vm = VM.getByUuid(vmc.xenConnection, record.uuid);
					if(vm.getPowerState(vmc.xenConnection)==Types.VmPowerState.RUNNING)
						vm.hardShutdown(vmc.xenConnection);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private class VMManageBehaviour extends OneShotBehaviour {
		private static final long serialVersionUID = 6671140607412559060L;

		public VMManageBehaviour(Agent a) {
			super(a);
		}

		@Override
		public void action() {
			synchronized(policy) {
				if(state == STATE.Normal) {
					if(!policy.MsgToRRA().isEmpty()) {
						policy.MsgToRRA().clear();
					}
					VMManagementDecision decision = policy.GetVMManagementDecision();
					
					if(decision!=null) {
						if(decision.command == VMManagementDecision.Command.START_VM) {
							try {
								decision.cluster.StartCluster();
								currentCluster = decision.cluster;
								state = STATE.Starting_VM;
							} catch(Exception e) {
								e.printStackTrace();
							}
						} else if(decision.command == VMManagementDecision.Command.CLOSE_VM) {
							ACLMessage msg = new ACLMessage(ACLMessage.INFORM);
							msg.addReceiver(decision.cluster.agentID);
							msg.setContent("TERMINATE");
							myAgent.send(msg);
							policy.GetRunningCluster().remove(decision.cluster);
							currentCluster = decision.cluster;
							state = STATE.Closing_VM;
						}
					}				
				} else if(state == STATE.Starting_VM) {
					if(policy.MsgToRRA().size()==0) return;
					ACLMessage msg = policy.MsgToRRA().remove(0);
					currentCluster.agentID = msg.getSender();
					
					currentCluster.agentContainer = currentCluster.jobType.ExtractContainer(msg);
					
					state = STATE.Normal;
					policy.GetRunningCluster().add(currentCluster);
					currentCluster = null;
				} else if(state == STATE.Closing_VM) {
					if(policy.MsgToRRA().size()==0) return;
					ACLMessage msg = policy.MsgToRRA().remove(0);
					
					if(currentCluster.agentID != msg.getSender()) {
						System.err.println("Got different agent id");
						return;
					}
					
					// TODO: kill the container
					
					state = STATE.Normal;
					policy.GetAvailableCluster().add(currentCluster);
					currentCluster = null;
				}
			}			
		}		
	}
}
