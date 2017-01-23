package tw.idv.ctfan.cloud.middleware;

import java.util.ArrayList;
import java.util.Map;

import com.xensource.xenapi.Types;
import com.xensource.xenapi.VM;

import tw.idv.ctfan.cloud.middleware.Cluster.JobType;
import tw.idv.ctfan.cloud.middleware.policy.MultiTypePolicy;
import tw.idv.ctfan.cloud.middleware.policy.Policy;
import tw.idv.ctfan.cloud.middleware.policy.Decision.VMManagementDecision;
import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.VMController;
import jade.core.AID;
import jade.core.Agent;
import jade.core.behaviours.Behaviour;
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
		Initializing, Normal, Starting_VM, Closing_VM
	};
	private STATE state;
	private ClusterNode currentCluster;
	
	public void setup() 
	{
		super.setup();
		
		synchronized(policy) {
			state = STATE.Initializing;
			currentCluster = null;
		
			policy.InitVMMasterList();
			policy.InitClusterList();
			
			// Close every VM
			InitAllVM();
			
			
		}
		
		tbf = new ThreadedBehaviourFactory();
		
		this.addBehaviour(tbf.wrap(new InitEnvironment(this)));
		
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
					if(!vm.getIsATemplate(vmc.xenConnection) &&
							!vm.getIsASnapshot(vmc.xenConnection) &&
							!vm.getIsControlDomain(vmc.xenConnection) &&
							!vm.getIsSnapshotFromVmpp(vmc.xenConnection))
								if(vm.getPowerState(vmc.xenConnection)==Types.VmPowerState.RUNNING)
									if(!vm.getNameLabel(vmc.xenConnection).equals("hdp218"))
										vm.hardShutdown(vmc.xenConnection);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private class InitEnvironment extends Behaviour {
		private static final long serialVersionUID = -4214491552096426986L;
		
		boolean doneYet = false;
				
		ArrayList<ClusterNode> openingCluster;
		int openingCount = 0;
		int doneCount = 0;
		
		public InitEnvironment(Agent a) {
			super(a);
			
			synchronized(policy) {
				openingCluster = new ArrayList<ClusterNode>();
				
				for(JobType jt: policy.GetJobTypeList()) {
					for(ClusterNode cn: policy.GetAvailableCluster()) {
						System.out.println(cn.jobType.getTypeName() + " " + jt.getTypeName());
						if(cn.jobType == jt) {
							openingCluster.add(cn);
							policy.GetAvailableCluster().remove(cn);
							System.out.println("Cluster " + openingCluster.size());
							
							break;
						}
					}
				}			
				System.out.println("" + openingCluster.size() + " Clusters will be started");
			}
		}

		@Override
		public void action() { synchronized(policy) {
			if(openingCount==doneCount) {
				policy.MsgToRRA().clear();
				if(openingCount==openingCluster.size()) {
					doneYet = true;
					return;
				}
				try{
					openingCluster.get(openingCount).StartCluster();
					openingCount++;
				} catch(Exception e) {
					System.out.println("Opening Cluster Problem");
					e.printStackTrace();
				}
			} else {
				if(policy.MsgToRRA().isEmpty()) return;
				ACLMessage msg = policy.MsgToRRA().remove(0);
				policy.MsgToRRA().clear();
				ClusterNode cn = openingCluster.get(doneCount);
				cn.agentID = msg.getSender().getName();		
				policy.GetRunningCluster().add(cn);
				doneCount++;
			}
		} }

		@Override
		public boolean done() {
			if(doneYet) state = STATE.Normal;
			return doneYet;
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
								policy.GetAvailableCluster().remove(currentCluster);
								state = STATE.Starting_VM;
							} catch(Exception e) {
								e.printStackTrace();
							}
						} else if(decision.command == VMManagementDecision.Command.CLOSE_VM) {
							ACLMessage msg = new ACLMessage(ACLMessage.INFORM);
							msg.addReceiver(new AID(decision.cluster.agentID, AID.ISGUID));
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
					currentCluster.agentID = msg.getSender().getName();	
					System.out.println(currentCluster.agentID + " is successfullly added");
//					currentCluster.agentContainer = currentCluster.jobType.ExtractContainer(msg);
					policy.GetRunningCluster().add(currentCluster);
					currentCluster = null;
					state = STATE.Normal;
				} else if(state == STATE.Closing_VM) {
					if(policy.MsgToRRA().size()==0) return;
					ACLMessage msg = policy.MsgToRRA().remove(0);
					
					if(currentCluster.agentID != msg.getSender().getName()) {
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
