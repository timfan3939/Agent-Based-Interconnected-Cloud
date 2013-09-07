package tw.idv.ctfan.cloud.middleware;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import com.xensource.xenapi.Types;
import com.xensource.xenapi.VM;

import tw.idv.ctfan.cloud.middleware.policy.MultiTypePolicy;
import tw.idv.ctfan.cloud.middleware.policy.Policy;
import tw.idv.ctfan.cloud.middleware.policy.Decision.VMManagementDecision;
import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.VMController;
import jade.content.lang.Codec;
import jade.content.lang.sl.SLCodec;
import jade.content.onto.Ontology;
import jade.content.onto.basic.Action;
import jade.core.AID;
import jade.core.Agent;
import jade.core.behaviours.Behaviour;
import jade.core.behaviours.CyclicBehaviour;
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

public class ResourceReconfigurationAgent extends Agent {

	private static final long serialVersionUID = 1L;	

	Policy policy = MultiTypePolicy.GetPolicy();
	
	
	ThreadedBehaviourFactory tbf;	
	ArrayList<VMController> vmControllerList;
	
	public void setup()
	{
		super.setup();
		
		vmControllerList = policy.InitVMMasterList();
		policy.InitClusterList();
		
		// Close every VM
		InitAllVM();
	}
	
	private void InitAllVM() {
		try {
			for(VMController vmc:vmControllerList) {
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
	
}
