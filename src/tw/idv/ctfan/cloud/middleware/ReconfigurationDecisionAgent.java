package tw.idv.ctfan.cloud.middleware;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import com.xensource.xenapi.Types;
import com.xensource.xenapi.VM;

import tw.idv.ctfan.cloud.middleware.policy.JavaPolicy;
import tw.idv.ctfan.cloud.middleware.policy.JavaPolicy1;
import tw.idv.ctfan.cloud.middleware.policy.JavaPolicy2;
import tw.idv.ctfan.cloud.middleware.policy.Policy;
import tw.idv.ctfan.cloud.middleware.policy.Decision.VMManagementDecision;
import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.VMMasterNode;

import jade.content.lang.Codec;
import jade.content.lang.sl.SLCodec;
import jade.content.onto.Ontology;
import jade.content.onto.basic.Action;
import jade.core.AID;
import jade.core.Agent;
import jade.core.behaviours.OneShotBehaviour;
import jade.core.behaviours.ThreadedBehaviourFactory;
import jade.core.behaviours.TickerBehaviour;
import jade.domain.JADEAgentManagement.JADEManagementOntology;
import jade.domain.JADEAgentManagement.KillContainer;
import jade.lang.acl.ACLMessage;

/*
 * This agent check if any cluster should shut down or boot up
 * This agent check if any agent (job) should be migrate to other cluster (but
 * not specify any agent).
 * The agent periodically ask AdministratorAgent to obtain the information of
 * all cluster and agent.  Then make decision.  If need to change, send INFORM
 * to ServiceMigrationAgent to migrate.
 */

public class ReconfigurationDecisionAgent extends Agent {

	private static final long serialVersionUID = 1L;
	
//	Policy policy = JobCountPolicy.GetPolicy();
//	Policy policy = JobSizePolicy.GetPolicy();
//	Policy policy = ExecutionTimePolicy.GetPolicy();
	Policy policy = JavaPolicy.GetPolicy();
//	Policy policy = JavaPolicy1.GetPolicy();
//	Policy policy = JavaPolicy2.GetPolicy();
	
	
	ThreadedBehaviourFactory tbf;	
	
	public void setup()
	{
		super.setup();		
		
		
		ArrayList<VMMasterNode> vmMasterList = policy.GetVMMaster();
		ArrayList<ClusterNode> clusterList = policy.GetAvailableCluster();
		
		vmMasterList.add(new VMMasterNode("10.133.200.16", "root", "unigrid", VMMasterNode.PRIVATE));
		vmMasterList.add(new VMMasterNode("10.133.200.9",  "root", "unigrid", VMMasterNode.PUBLIC));
		
		try {
			// Get VMs
			for(int i=0; i<vmMasterList.size(); i++) {
				VMMasterNode vmMaster = vmMasterList.get(i);
				Map<VM, VM.Record> VMs = VM.getAllRecords(vmMaster.xenConnection);
				for(VM.Record record : VMs.values()) {
					VM vm = VM.getByUuid(vmMaster.xenConnection, record.uuid);
					if(!vm.getIsATemplate(vmMaster.xenConnection)&&
					   !vm.getIsASnapshot(vmMaster.xenConnection)&&
					   !vm.getIsControlDomain(vmMaster.xenConnection)&&
					   !vm.getIsSnapshotFromVmpp(vmMaster.xenConnection)) {
						if(vm.getNameLabel(vmMaster.xenConnection).startsWith("hdp")) {
							clusterList.add(new ClusterNode(vmMaster,
															vm.getUuid(vmMaster.xenConnection),
															vm.getNameLabel(vmMaster.xenConnection),
															vm.getVCPUsMax(vmMaster.xenConnection),
															vm.getMemoryDynamicMax(vmMaster.xenConnection),
															100));
						}
					}
				}				
			}
			// Sorting the VM;
			Collections.sort(clusterList);
			
			try {
				for(ClusterNode cn : clusterList) {
					VM vm = VM.getByUuid(cn.vmMaster.xenConnection, cn.vmUUID);
					System.out.print("Found VM " + vm.getNameLabel(cn.vmMaster.xenConnection));
					if(vm.getPowerState(cn.vmMaster.xenConnection)==Types.VmPowerState.RUNNING) {
						System.out.println(" Closing ...");
						vm.hardShutdown(cn.vmMaster.xenConnection);
					} else {
						System.out.println(" Closed");
					}
				}
			}
			catch(Exception e) {
				System.err.println("Error Finding Clusters");
				e.printStackTrace();
			}
			
			
		} catch (Exception e) {
			System.out.println("Error Getting VM");
			e.printStackTrace();
		}		
		
		tbf = new ThreadedBehaviourFactory();
		
		this.addBehaviour(tbf.wrap(new TickerBehaviour(this, 3000) {
			private static final long serialVersionUID = 1L;
			private VMManageBehaviour running = null;
			
			@Override
			protected void onTick() {
				if(running==null || running!=null&&running.done()) {
					running = new VMManageBehaviour(myAgent);
					myAgent.addBehaviour(tbf.wrap(running));
				}
			}
		}));
	}
	
	public VMManageBehaviour VMManageBehaviourOnlyInstance = null;
	
	private class VMManageBehaviour extends OneShotBehaviour {

		private static final long serialVersionUID = 1L;
		
		VMManageBehaviour(Agent a) {
			super(a);
		}

		@Override
		public void action() {
			if(VMManageBehaviourOnlyInstance == null) {
				VMManageBehaviourOnlyInstance =this;
				
				VMManagementDecision decision = policy.GetVMManagementDecision();
				
				if(decision!=null) {
					if(decision.command == VMManagementDecision.START_VM) {
						System.out.println("===Starting New VM===");
						try {
							VM vm = VM.getByUuid(decision.cluster.vmMaster.xenConnection, decision.cluster.vmUUID);
							vm.start(decision.cluster.vmMaster.xenConnection, false, false);
						} catch (Exception e) {
							System.err.println("ReconfigurationDecisionAgent : Error while Starting VM");
							e.printStackTrace();
						}
					}
					else if(decision.command == VMManagementDecision.CLOSE_VM) {
						try {
							ACLMessage msg = new ACLMessage(ACLMessage.INFORM);
							AID recv = new AID(decision.cluster.name + "@" + decision.cluster.address + ":1099/JADE", AID.ISGUID);
							msg.addReceiver(recv);
							msg.setContent("TERMINATE");
						} catch(Exception e) {
							System.err.println("ReconfigurationDecisionAgent : Error while Closing VM");
							e.printStackTrace();
						}
						
						// do nothing right now
					}					
				}				
				VMManageBehaviourOnlyInstance = null;
			}
		}		
	}	
}
