package tw.idv.ctfan.cloud.middleware;

import java.util.ArrayList;
import java.util.Map;

import com.xensource.xenapi.Types;
import com.xensource.xenapi.VM;

import tw.idv.ctfan.cloud.middleware.policy.MultiTypePolicy;
import tw.idv.ctfan.cloud.middleware.policy.Policy;
import tw.idv.ctfan.cloud.middleware.policy.data.VMController;
import jade.core.Agent;
import jade.core.behaviours.ThreadedBehaviourFactory;

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
		
		// Start up one cluster for each type of job
		
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
