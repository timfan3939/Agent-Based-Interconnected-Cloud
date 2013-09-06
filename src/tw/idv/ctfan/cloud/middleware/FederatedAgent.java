package tw.idv.ctfan.cloud.middleware;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

import tw.idv.ctfan.cloud.middleware.policy.*;
import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.HadoopJobNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JavaJobNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNodeBase;
import tw.idv.ctfan.cloud.middleware.policy.data.VMMasterNode;
import tw.idv.ctfan.cloud.middleware.policy.Decision.DispatchDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.MigrationDecision;
import tw.idv.ctfan.cloud.middleware.policy.Decision.VMManagementDecision;

import jade.content.lang.Codec;
import jade.content.lang.sl.SLCodec;
import jade.content.onto.Ontology;
import jade.content.onto.basic.Action;
import jade.core.AID;
import jade.core.Agent;
import jade.core.ContainerID;
import jade.core.behaviours.Behaviour;
import jade.core.behaviours.CyclicBehaviour;
import jade.core.behaviours.OneShotBehaviour;
import jade.core.behaviours.ThreadedBehaviourFactory;
import jade.core.behaviours.TickerBehaviour;
import jade.domain.JADEAgentManagement.JADEManagementOntology;
import jade.domain.JADEAgentManagement.KillContainer;
import jade.lang.acl.ACLMessage;
import jade.lang.acl.MessageTemplate;

import com.xensource.xenapi.Types;
import com.xensource.xenapi.VM;

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
	public static final String NAME = "FederatedAgent";
	
	ThreadedBehaviourFactory tbf;
	Policy policy;
	
	/*
	 * These two are used to kill the container.
	 */
	Codec codec;
	Ontology jmo;
	
	ArrayList<VMMasterNode> vmMasterList;
	
	/**
	 * Normal Setup Overriding function
	 * 
	 * @author C.T.Fan
	 */
	public void setup() {
		super.setup();
		
		tbf = new ThreadedBehaviourFactory();
		
		codec = new SLCodec();
		jmo = JADEManagementOntology.getInstance();
		this.getContentManager().registerLanguage(codec);
		this.getContentManager().registerOntology(jmo);
		
		// Policy defination
//		policy = JobCountPolicy.GetPolicy();
//		policy = JobSizePolicy.GetPolicy();
//		policy = ExecutionTimePolicy.GetPolicy();
//		policy = JavaPolicy.GetPolicy();
//		policy = JavaPolicy1.GetPolicy();
//		policy = JavaPolicy2.GetPolicy();
		policy = MultiTypePolicy.GetPolicy();
		
		vmMasterList = policy.GetVMMaster();
		ArrayList<ClusterNode> clusterList = policy.GetAvailableCluster();
		
		vmMasterList.add(new VMMasterNode("10.133.200.4", "root", "unigrid", VMMasterNode.PRIVATE));
		
		try {
			for (int i=0; i<vmMasterList.size(); i++) {
				VMMasterNode vmMaster = vmMasterList.get(i);
				Map<VM, VM.Record> VMs = VM.getAllRecords(vmMaster.xenConnection);
				
				for (VM.Record record : VMs.values()) {
					VM vm = VM.getByUuid(vmMaster.xenConnection, record.uuid);
					if (!vm.getIsATemplate(vmMaster.xenConnection)&&
					    !vm.getIsASnapshot(vmMaster.xenConnection)&&
					    !vm.getIsControlDomain(vmMaster.xenConnection)&&
					    !vm.getIsSnapshotFromVmpp(vmMaster.xenConnection)) {
						if (vm.getNameLabel(vmMaster.xenConnection).startsWith("hdp201")||
							vm.getNameLabel(vmMaster.xenConnection).startsWith("hdp202")||
							vm.getNameLabel(vmMaster.xenConnection).startsWith("hdp205")||
							vm.getNameLabel(vmMaster.xenConnection).startsWith("hdp206")) {
							
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
			Collections.sort(clusterList);
			
			try {
				for (ClusterNode cn : clusterList) {
					VM vm = VM.getByUuid(cn.vmMaster.xenConnection, cn.vmUUID);
					System.out.print("Found VM " + vm.getNameLabel(cn.vmMaster.xenConnection));
					if (vm.getPowerState(cn.vmMaster.xenConnection)==Types.VmPowerState.RUNNING) {
						System.out.println(" Closing ...");
						vm.hardShutdown(cn.vmMaster.xenConnection);
					}
					else {
						System.out.println(" Closed");
					}
				}
				System.out.println("VM's Ready");
			}
			catch (Exception e) {
				System.err.println("Error Finding Clusters");
				e.printStackTrace();
			}
			
		} catch (Exception e) {
			System.out.println("Error Getting VM");
			e.printStackTrace();
		}
		
		// AdministratorAgent
		AddTbfBehaviour(new ListeningBehaviour(this));
		AddTbfBehaviour(new HTTPServerBehaviour2(this, policy));
		AddTbfBehaviour(new SubmitBehaviour(this));
		
		// MigrationAgent
		AddTbfBehaviour(new TickerBehaviour(this, 3000){
			private static final long serialVersionUID = 1L;
			private ServiceManageBehaviour running = null;
			
			@Override
			protected void onTick() {
				if (running == null || (running!=null&&running.done())) {
					running = new ServiceManageBehaviour(myAgent);
					AddTbfBehaviour(running);
				}
			}
		});
		
		// ReconfigurationDecisionAgent
		AddTbfBehaviour(new TickerBehaviour(this, 3000){
			private static final long serialVersionUID = 1L;
			private VMManageBehaviour running = null;
			
			@Override
			protected void onTick() {
				if (running == null || (running!=null&&running.done())) {
					running = new VMManageBehaviour(myAgent);
					AddTbfBehaviour(running);
				}
			}
		});
		
	}
	
	/**
	 * This function helps HTTPServerBehaviour2 to wrap newly added behaviour
	 * 
	 * @param b Behaviour that going to be wrapped
	 * @see HTTPServerBehaviour2
	 */
	public final void AddTbfBehaviour(Behaviour b) {
		this.addBehaviour(this.tbf.wrap(b));
	}
	
	/**
	 * This function helps HTTPServerBehaviour2 to wrap newly added Job
	 * 
	 * @param newJob The job being added
	 * @see HTTPServerBehaviour2
	 */
	public final void SubmitJob(JobNodeBase newJob) {
		this.addBehaviour(this.tbf.wrap(new GetJobInfoBehaviour(this, newJob)));
	}
	
	// Behaviours from ReconfigurationDecisionAgent
	
	
	/**
	 * This Behaviour will grabs the information of the job.
	 * 
	 * @author DMCLAB
	 *
	 */
	private class GetJobInfoBehaviour extends OneShotBehaviour {
		private static final long serialVersionUID = 1L;
		JobNodeBase m_job;
		
		/**
		 * Init
		 * 
		 * @param a The performing agent
		 * @param jn The job.  The job must extends the JobNodeBase
		 * 
		 * @see JobNodeBase
		 */
		GetJobInfoBehaviour(Agent a, JobNodeBase jn) {
			super(a);
			m_job = jn;
		}

		@Override
		public void action() {
			if(m_job.jobType.matches(HadoopJobNode.JOBTYPENAME)) {
				// TODO: something about hadoop job
			}
			else if(m_job.jobType.matches(JavaJobNode.JOBTYPENAME)) {
				JavaJobNode jJobNode = (JavaJobNode) m_job;
				jJobNode.jobSize = Long.parseLong(jJobNode.command);
			}
			
			policy.GetWaitingJob().add(m_job);
			
			// TODO add new job request behaviour
		}
		
	}
	
	/**
	 * This Behaviour receives the heartbeats from all the clients.
	 * <p>
	 * Heartbeats have two kind of Performative: CONFIRM and REQUEST
	 * <p>
	 * Confirm Message will be like this:<br>
	 * 		cluster <agent's name> <agent's container name> <agent's IP> \n<br>
	 * 		load <cluster load>\n<br>
	 * 		job <job type> <job's name> finished \n<br>
	 * 		job <job type> <job's name> running <last heartbeat time <hasBeenExecuted> <map status> <reduce status> \n<br>
	 * 		job <job type> <job's name> waiting <last heartbeat time> <finished time><br>
	 * <p>
	 * Request Message will be like this:<br>
	 * 		Request Close cluster <agent's name> <agent's container name> <agent's IP>
	 * 
	 * @author DMCLAB
	 *
	 */
	private class ListeningBehaviour extends CyclicBehaviour {
		private static final long serialVersionUID = 1L;
		
		public ListeningBehaviour (Agent agent) {
			super(agent);
		}

		@Override
		public void action() {
			
			try {
				ACLMessage msg = myAgent.receive(MessageTemplate.MatchAll());
				
				if(msg == null) {
					block();
					return;
				}
				
				switch (msg.getPerformative()) {
				case ACLMessage.CONFIRM: {
					String content = msg.getContent();
					String[] subContent = content.split("\n");
					String[] line = subContent[0].split(" ");
					ClusterNode cn = null;
					JobNodeBase jn = null;
					boolean found = false;
					
					if(line[0].matches("cluster")) {
						found = false;
						for(int i=0; i<policy.GetRunningCluster().size()&&!found; i++) {
							found = policy.GetRunningCluster().get(i).compare(line[1], line[2], line[3]);
							if(found)
								cn = policy.GetRunningCluster().get(i);
						}
						
						if(!found) {
							cn = new ClusterNode(line[1], line[2], line[3]);
							policy.OnNewClusterArrives(cn);
						}
						else {
							line = subContent[1].split(" ");
							
							for(int i=0; i<line.length; i++) {
								if(line[i].matches("load")) {
									i++;
									cn.load = Integer.parseInt(line[i]);
								}
								else if (line[i].matches("maxMap")) {
									i++;
									cn.maxMapSlot = Integer.parseInt(line[i]);
								}
								else if(line[i].matches("maxReduce")) {
									i++;
									cn.maxReduceSlot = Integer.parseInt(line[i]);
								}
							}
						}
						if (subContent.length > 2) {
							for (int i=2; i<subContent.length; i++) {
								found = false;
								line = subContent[i].split(" ");
								
								if (line[0].matches("job")) {
									for(int j=0; j<policy.GetRunningJob().size(); j++) {
										if (Long.parseLong(line[2].substring(3))==policy.GetRunningJob().get(j).UID) {
											jn = policy.GetRunningJob().get(j);
											found = true;
										}
									}
									
									if (found) {
										jn.currentPosition = cn;
										if(line[3].matches("running")) {
											jn.lastExist = Long.parseLong(line[4]);
											jn.jobStatus = HadoopJobNode.RUNNING;
											jn.hasBeenExecutedTime = Long.parseLong(line[5]);
											if (line[1].matches(HadoopJobNode.JOBTYPENAME)) {
												((HadoopJobNode)jn).mapStatus = Integer.parseInt(line[6]);
												((HadoopJobNode)jn).reduceStatus = Integer.parseInt(line[7]);
											}
											else if (line[1].matches(JavaJobNode.JOBTYPENAME)) {
												// TODO: some other parameters for java jobs
											}
										}
										else if (line[3].matches("waiting")) {
											jn.lastExist = Long.parseLong(line[4]);
											jn.jobStatus = HadoopJobNode.WAITING;
										}
										else if (line[3].matches("finished")) {
											System.out.println(myAgent.getLocalName() + ": got finish job");
											policy.GetRunningJob().remove(jn);
											policy.GetFinishJob().add(jn);
											jn.finishTime = System.currentTimeMillis();
											
											jn.executeTime = Long.parseLong(line[4]);
											
											System.out.println("Difference: " + ((double)jn.predictTime - (double)jn.executeTime)/(double)jn.executeTime);
											System.out.println("Setting: " + jn.command + " time: " + jn.executeTime);
											jn.jobStatus = HadoopJobNode.FINISHED;
											jn.currentPosition = new ClusterNode(jn.currentPosition.maxMapSlot, jn.currentPosition.maxReduceSlot);
											
											// TODO: add finished output file size procedure
										}
									}
									else {
										System.err.println("Error");
										System.err.println(subContent[i]);
									}
								}
							}
						}
					}
				}
					break;
				case ACLMessage.REQUEST: {
					String content = msg.getContent();
					System.out.println(content);
					String[] line = content.split("\n");
					String[] subContent = line[0].split(" ");
					if(line.length==2)
					if(subContent.length==5)
					if(subContent[0].matches("Close"));
					if(subContent[1].matches("cluster"));
						policy.OnOldClusterLeaves(new ClusterNode(subContent[2], subContent[3], subContent[4]));
						AddTbfBehaviour(new ContainerCloseBehaviour(myAgent, line[1]));
				}
					break;
				case ACLMessage.INFORM: {
					//if(msg.getSender() == myAgent.getAMS()){ 
					if(onlyVMCloseBehaviourInstance != null){
						AddTbfBehaviour(onlyVMCloseBehaviourInstance);
						onlyVMCloseBehaviourInstance = null;
					}
				}
					break;
				default:
					System.out.println("Got Message");
					System.out.println(msg.getContent());
					break;
				}
			}
			catch(Exception e) {
				System.err.println("Error");
				e.printStackTrace();
			}
			
		}
		
	}
	
	private class SubmitBehaviour extends CyclicBehaviour {
		private static final long serialVersionUID = 1L;
		ServerSocket server;
		
		SubmitBehaviour(Agent a) {
			super(a);
			
			try {
				server = new ServerSocket(50031);
			}
			catch (Exception e){
				System.err.println("Creating Socket error");
				e.printStackTrace();
				myAgent.doDelete();
			}
					
		}
		
		
		@Override
		public void action() {
			try {
				Socket s = server.accept();
				
				System.out.println(myAgent.getLocalName() + ": Got Client");
				
				String host = s.getInetAddress().getHostAddress();
				InputStream input = s.getInputStream();
				
				String jobInputFolder = null;
				String jobOutputFolder = null;
				String jobInputData = null;
				String jobParameter = null;
				byte[] jobBinaryFile = null;
				String jobName = null;
				String jobDeadline = null;
				
				String line = "";
				String head = "";
				String tail = "";
				
				String jobType = "";
				
				int ch;
				
				byte[] buff = new byte[0x1000];
				
				int bufflen = 0;
				
				while (true) {
					bufflen = 0;
					while ( (ch = input.read()) >=0 && ch!= '\n') {
						buff[bufflen] = (byte) ch;
						bufflen++;
					}
					
					if(ch!='\n') continue;
					
					line = new String(buff, 0, bufflen);
					
					head = line.substring(0, line.indexOf(':'));
					tail = line.substring(line.indexOf(':')+1);
					
					if (!jobType.isEmpty()) {
						if (head.matches("Parameter")) {
							jobParameter = tail;
							bufflen = 0;
						}
						else if (head.matches("BinaryDataLength")) {
							int jobLength = Integer.parseInt(tail);
							jobBinaryFile = new byte[jobLength];
							int read = input.read(jobBinaryFile, 0, jobLength);
							
							while(read<jobLength && (ch=input.read(buff))>0) {
								for(int i=0; i<ch; i++) {
									jobBinaryFile[read+1] = buff[i];
								}
								read += ch;
							}
							break;
						}
						else if (jobType.matches(HadoopJobNode.JOBTYPENAME)) {
							if(head.matches("InputFolder")) {
								jobInputFolder = tail;
								bufflen = 0;
							}
							else if (head.matches("OutputFolder")) {
								jobOutputFolder = tail;
								bufflen = 0;
							}
						}
						else if (jobType.matches(JavaJobNode.JOBTYPENAME)) {
							if (head.matches("InputFile")) {
								jobInputData = tail;
								bufflen = 0;
							}
							else if (head.matches("name")) {
								jobName = tail;
								bufflen = 0;
							}
							else if (head.matches("deadline")) {
								jobDeadline = tail;
								bufflen = 0;
							}
						}
					}
					else {
						if (head.matches("JobType")) {
							jobType = tail;
							bufflen = 0;
						}
						else {
							System.err.println("JobType Should Comes First");
						}
					}
				}				
				s.close();
				
				if(!jobType.isEmpty() && jobBinaryFile!= null) {
					JobNodeBase newJob = null;
					if(jobType.matches(HadoopJobNode.JOBTYPENAME)) {
						HadoopJobNode hadoopNewJob = new HadoopJobNode(jobType, host, jobParameter, jobBinaryFile, jobInputFolder, jobOutputFolder);
						newJob = hadoopNewJob;
					}
					else if (jobType.matches(JavaJobNode.JOBTYPENAME)) {
						JavaJobNode javaNewJob = new JavaJobNode(jobType, host, jobParameter, jobBinaryFile);
						javaNewJob.inputData = jobInputData;
						javaNewJob.jobName = jobName;
						javaNewJob.deadline = Long.parseLong(jobDeadline);
						newJob = javaNewJob;
					}
					
					AddTbfBehaviour(new GetJobInfoBehaviour(myAgent, newJob));
				}
			}			
			catch (Exception e) {
			}
			
		}
	}
	
	
	// Behaviours from ServiceMigrationAgent
	
	/**
	 * This only instance helps to check if only one of this behaviour is being fired
	 */
	public ServiceManageBehaviour ServiceManageBehaviourOnlyInstance = null;
	
	private class ServiceManageBehaviour extends OneShotBehaviour {
		private static final long serialVersionUID = 1L;
		
		public ServiceManageBehaviour(Agent a) {
			super(a);
		}
		
		@Override
		public void action() {
			if (ServiceManageBehaviourOnlyInstance == null) {
				ServiceManageBehaviourOnlyInstance = this;
				
				ACLMessage msg;
				
				MigrationDecision decision = null;
				if ((decision = policy.GetMigrationDecision()) != null) {
					// TODO Migration Decision, not used in hybrid cloud right now
					decision = null;
				}
				else if (policy.GetWaitingJob().size()>0) {
					DispatchDecision dispatchDecision = policy.GetNewJobDestination();
					
					if (dispatchDecision!=null) {
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
						
						msg.setByteSequenceContent(s.toByteArray());
						
						myAgent.send(msg);
						
						jn.binaryFile = null;
						
						policy.GetWaitingJob().remove(jn);
						policy.GetRunningJob().add(jn);
						
					}
				}
				ServiceManageBehaviourOnlyInstance = null;
				
			}
			
		}
		
	}
	
	// Behaviour from ReconfigurationDecisionAgent
	
	private VMManageBehaviour VMManageBehaviourOnlyInstance = null;
	
	private class VMManageBehaviour extends OneShotBehaviour {
		private static final long serialVersionUID = 1L;
		
		VMManageBehaviour(Agent a) {
			super(a);
		}
		
		@Override
		public void action() {
			if (VMManageBehaviourOnlyInstance==null) {
				VMManageBehaviourOnlyInstance = this;
				
				VMManagementDecision decision = policy.GetVMManagementDecision();
				
				if (decision!=null) {
					if (decision.command == VMManagementDecision.START_VM) {
						System.out.println("===Starting New VM===");
						try {
							VM vm = VM.getByUuid(decision.cluster.vmMaster.xenConnection, decision.cluster.vmUUID);
							vm.start(decision.cluster.vmMaster.xenConnection, false, false);
						}
						catch (Exception e) {
							System.err.println("ReconfigurationDecisionAgent : Error while Starting VM");
							e.printStackTrace();
						}
					}
					else if (decision.command == VMManagementDecision.CLOSE_VM) {
						try {
							ACLMessage msg = new ACLMessage(ACLMessage.INFORM); 
								AID recv = new AID(decision.cluster.name + "@" + decision.cluster.address + ":1099/JADE", AID.ISGUID);
								msg.addReceiver(recv);
								msg.setContent("TERMINATE");
								myAgent.send(msg);
								//AddTbfBehaviour(new VMCloseBehaviour(myAgent, decision.cluster));	
								onlyVMCloseBehaviourInstance = new VMCloseBehaviour(myAgent, decision.cluster);
						} catch (Exception e) {
							System.err.println("ReconfigurationDecisionAgent : Error while Closing VM");
							e.printStackTrace();
						}
					}
				}
				VMManageBehaviourOnlyInstance = null;
			}
			
		}
	}
	
	/*
	 * This behaviour sends the message to AMS to kill the specific container
	 */
	private class ContainerCloseBehaviour extends OneShotBehaviour {
		private static final long serialVersionUID = 1L;
		
		String containerName;
		
		public ContainerCloseBehaviour(Agent a, String ContainerName) {
			super(a);
			containerName = ContainerName;
		}

		@Override
		public void action() {
			try {
				KillContainer kill = new KillContainer();
				kill.setContainer(new ContainerID(containerName, null));
				
				ACLMessage msg = new ACLMessage(ACLMessage.REQUEST);
				msg.addReceiver(getAMS());
				msg.setLanguage(codec.getName());
				msg.setOntology(jmo.getName());
				myAgent.getContentManager().fillContent(msg, new Action(myAgent.getAID(), kill));
				myAgent.send(msg);
				
			}
			catch(Exception e) {
				System.out.println("Error while killing the container");
				e.printStackTrace();
			}
		}

	}
	
	private VMCloseBehaviour onlyVMCloseBehaviourInstance = null;
	
	private class VMCloseBehaviour extends Behaviour {
		private static final long serialVersionUID = 1L;
		
		ClusterNode cluster;
		boolean doneYet = false;
		int count = 0;
		
		VMCloseBehaviour (Agent a, ClusterNode cn) {
			super(a);
			this.cluster = cn;
		}
		
		@Override
		public void action() {
			block(5000);
			System.out.println("Try " + count++ + " times");
			
			for (ClusterNode cn : policy.GetAvailableCluster()) {
				if (cn.compare(cluster)) {
					try {
						VM vm = VM.getByUuid(cn.vmMaster.xenConnection, cn.vmUUID);
						vm.hardShutdown(cn.vmMaster.xenConnection);
						doneYet = true;
						return;
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}

		@Override
		public boolean done() {
			return doneYet;
		}
		
	}
	
}
