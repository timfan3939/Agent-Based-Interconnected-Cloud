package tw.idv.ctfan.cloud.middleware;

import tw.idv.ctfan.cloud.middleware.policy.*;
import jade.core.Agent;
import jade.core.behaviours.*;

/**
 * This agent bundles all other agents, like AdministratorAgent,
 * ReconfigurationDecisionAgent, and Service MigrationAgent.
 * This agent also uses composite behaviour to help the agent to goes smoothly.
 * <p>
 * Behaviours copied or re-writtened from ReconfigurationDecisionAgent:
 * <ul>
 * <li>Init Behaviour Get Virtual Machines' List from Virtual Machine Managers
 * <li>TickerBehaviour that Fire the Requesting new Decision from policy
 * <li>Behaviour that Requesting Decision from policy
 * <li>Behaviour that closing a Virtual Machine
 * <li>Behaviour that starting a Virtual Machine
 * </ul>
 * <p>
 * Behaviours copied or re-writtened from AdministratorAgent:
 * <ul>
 * <li>CylicBehaviour that accepting Submited Job Via Program
 * <li>Behaviour that Getting the informations about the job submitted
 * <li>CylicBehaviour that listening to the messages from other agents
 * <li>HTTPServerBehaviour
 * </ul>
 * <p>
 * Behaviours copied or re-writtened from ServiceMigrationAgent:
 * <ul>
 * <li>	TickerBehaviour that Fire the Requsting new Decison from policy
 * <li> Behaviour that Requesting Decision from policy
 * </ul>
 * 
 * @author C.T.Fan
 * @see ReconfigurationDecisionAgent
 * @see AdministratorAgent
 * @see ServiceMigrationAgent
 *
 */
public class FederatedAgent extends Agent {
	private static final long serialVersionUID = 1L;
	
	ThreadedBehaviourFactory tbf;
	Policy policy;
	
	/**
	 * Normal Setup Overriding function
	 * 
	 * @author C.T.Fan
	 */
	public void setup() {
		super.setup();
		
		tbf = new ThreadedBehaviourFactory();
		
//		policy = JobCountPolicy.GetPolicy();
//		policy = JobSizePolicy.GetPolicy();
//		policy = ExecutionTimePolicy.GetPolicy();
		policy = JavaPolicy.GetPolicy();
//		policy = JavaPolicy1.GetPolicy();
//		policy = JavaPolicy2.GetPolicy();
	}
	
	// Behaviours from ReconfigurationDecisionAgent
	
}
