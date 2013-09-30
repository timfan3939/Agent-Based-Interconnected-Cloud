package tw.idv.ctfan.cloud.middleware.Cluster;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;

import jade.core.AID;
import jade.core.Agent;
import jade.core.behaviours.Behaviour;
import jade.core.behaviours.CyclicBehaviour;
import jade.core.behaviours.OneShotBehaviour;
import jade.core.behaviours.TickerBehaviour;
import jade.lang.acl.ACLMessage;
import jade.lang.acl.MessageTemplate;
import jade.wrapper.StaleProxyException;

public abstract class AdminAgent extends Agent {

	/**
	 * What, this is just a serial version UID, nothing special
	 */
	private static final long serialVersionUID = 1L;
	protected final JobType m_jobType;
	
	protected String m_jarPath = "/home/hadoop/ctfan";
	protected String m_masterIP;
	
	protected int maxExecuteJobNumber = 1;
	
	private boolean firstHeartBeat = false;
	private int startHeartBeat = 5;

	protected enum JOB_STATUS {
		Waiting, Running, Finished;
	}
	
	protected class JobListNode {
		public String name;
		public long lastExist;
//		public int hasBeenExecute;
		public long executedTime = 0;
		JOB_STATUS status;
		public HashMap<String, String> attributes;
		public byte[] binaryFile;
		
		public JobListNode(String name) {
			this.name = name;
			this.attributes = new HashMap<String, String>();
			this.binaryFile = null;
			lastExist = -1;
//			hasBeenExecute = -1;
			status = JOB_STATUS.Waiting;
		}
		
		public void SetExist() {
			lastExist = System.currentTimeMillis();
		}
	}
	
	protected ArrayList<JobListNode> m_jobList = new ArrayList<JobListNode>();
	
	public AdminAgent(JobType jt) {
		m_jobType = jt;
	}
	
	public void setup() {
		super.setup();		
		
		// TODO: command line
		m_masterIP = "120.126.145.102";
		
		this.addBehaviour(new InitilizeClusterBehaviour(this));
	}
	
	private class InitilizeClusterBehaviour extends Behaviour {
		private static final long serialVersionUID = -5196327916599266133L;
		
		boolean doneYet = false;
		AdminAgent theAgent;

		public InitilizeClusterBehaviour(AdminAgent a){
			super(a);
			theAgent = a;
		}		

		@Override
		public void action() {
			if( !doneYet && ((doneYet = InitilizeCluster()) == true) ) {
				System.out.println("Starting other behaviour");
				theAgent.addBehaviour(new MessageListeningBehaviour(theAgent));
				theAgent.addBehaviour(new HeartBeatBehaviour(theAgent, 3000));
			}
		}

		@Override
		public boolean done() {
			return doneYet;
		}
		
	}

	protected String getJarPath() {
		return m_jarPath;
	}

	protected void setJarPath(String mJarPath) {
		m_jarPath = mJarPath;
	}

	protected String getMasterIP() {
		return m_masterIP;
	}

	protected void setMasterIP(String mMasterIP) {
		m_masterIP = mMasterIP;
	}

	protected int getMaxExecuteJobNumber() {
		return maxExecuteJobNumber;
	}

	protected void setMaxExecuteJobNumber(int maxExecuteJobNumber) {
		this.maxExecuteJobNumber = maxExecuteJobNumber;
	}
	
	private class MessageListeningBehaviour extends CyclicBehaviour {
		private static final long serialVersionUID = 1L;
		
		public MessageListeningBehaviour(Agent agent){
			super(agent);
		}

		/*
		 * Request	: new job
		 * Propose	: job Execution (but have to wait)
		 * Inform	: job execution/migration
		 * Confirm	: job exists 
		 */
		
		@Override
		public void action() {
			try {
				ACLMessage msg = myAgent.receive(MessageTemplate.MatchAll());
//				JobListNode JNode;
				
				if(msg==null) {
					block();
					return;
				}
				
				switch(msg.getPerformative()) {
				case ACLMessage.REQUEST: {
					byte[] program = msg.getByteSequenceContent();
					
					myAgent.addBehaviour(new NewJobBehaviour(myAgent, program));
				} break;
				case ACLMessage.INFORM: {
					String info = msg.getContent();
					if(info.matches("MIGRATION")) {
						// TODO: add some migration behaviour here
					} else if(info.matches("TERMINATE")) {
						myAgent.addBehaviour(new TerminateVMBehaviour(myAgent));
					}
				} break;
				case ACLMessage.PROPOSE:
				case ACLMessage.CONFIRM: synchronized(m_jobList) {
					for(JobListNode jn:m_jobList) {
						if(jn.name.compareTo(msg.getSender().getLocalName()) == 0) {
							jn.SetExist();
							if(msg.getContent().compareTo("WAITING")==0) {
								jn.status = JOB_STATUS.Waiting;
							} else if(msg.getContent().compareTo("FINISHED") == 0) {
								jn.status = JOB_STATUS.Finished;
							} else {
								jn.status = JOB_STATUS.Running;
							}
							break;
						}
					}
				} break;
				}				
				
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	private class NewJobBehaviour extends Behaviour {
		private static final long serialVersionUID = 1L;
		
		byte[] m_data;
		boolean hasParsed = false;  // avoid init agent error
		boolean doneYet = false;
		
		JobListNode newJob;

		public NewJobBehaviour(Agent a, byte[] program) {
			super(a);
			
			m_data = program.clone();
		}
		
		@Override
		public void action() {
			if(!hasParsed) {
				// Parse the data with the following function
				int c=0;
				int buffLen;
				byte[] buff = new byte[0x400], binary = null;
							
				newJob = new JobListNode("");
				
				ByteArrayInputStream dataInput = new ByteArrayInputStream(m_data);
				
				while(dataInput.available()>0) {
					buffLen =0;
					while( (c=dataInput.read()) != '\n' ) {
						buff[buffLen] = (byte)c;
						buffLen++;
					}
					
					String line = new String (buff, 0, buffLen);
					int index = line.indexOf(":");
					String head = line.substring(0, index);
					String tail = line.substring(index+1);
					System.out.println(line);
					
					if(head.matches("UID")) {
						newJob.name = tail;
					} else if(head.matches("BinaryDataLength")){
						try {
							int jobLength = Integer.parseInt(tail);
							binary = new byte[jobLength];
							dataInput.read(binary, 0, jobLength);
						} catch (Exception e) {
							e.printStackTrace();
						}
						newJob.binaryFile = binary;
					} else {
						OnDecodeNewJob(newJob, head, tail);
					}
				}
				hasParsed = true;
			}
			if(newJob==null) return;
			synchronized(m_jobList) {
				try {
					File f = new File(m_jarPath + "/job" + newJob.name + m_jobType.GetExtension());
					if(!f.exists()) {
						FileOutputStream output = new FileOutputStream(f);
						output.write(newJob.binaryFile);
						output.close();
						newJob.binaryFile = null;
					}
					
					m_jobList.add(newJob);
					
					ArrayList<String> cmd = new ArrayList<String>();
					cmd.add(myAgent.getLocalName());
					cmd.add(m_jarPath);
					cmd.add("job" + newJob.name + m_jobType.GetExtension());
					cmd.add(OnEncodeNewJobAgent(newJob));
					
					myAgent.getContainerController().createNewAgent(newJob.name, GetJobAgentClassName() ,cmd.toArray()).start();
					System.out.println("===== Agent " + newJob.name + " Start=====");
					doneYet = true;				
				} catch (StaleProxyException e) {
					System.err.println("Agent Exception");
					e.printStackTrace();
					block(5000);
					return;
				}
				catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		@Override
		public boolean done() {
			return doneYet;
		}		
	}
	
	private class HeartBeatBehaviour extends TickerBehaviour {
		private static final long serialVersionUID = 1L;
		
		private long lastAskExecuteJob;

		public HeartBeatBehaviour(AdminAgent agent, int i) {
			super(agent, i);
			lastAskExecuteJob = 0;
		}

		// Divided into two parts. One is report part, the other one is activate job part.
		
		// Line 1: cluster position information
		// Line 2: cluster information (Encode/Decode by user, can update ClusterNode at server side)
		// Line 3: job information
		// Line 4: job information of the last line, used to update the values in JobNode at server side
		@Override
		protected void onTick(){ synchronized(m_jobList) {
			ACLMessage heartBeat = new ACLMessage(ACLMessage.CONFIRM);
			
			if(!firstHeartBeat) {
				heartBeat.setPerformative(ACLMessage.REQUEST);
				firstHeartBeat = true;
			} else if(startHeartBeat>0) {
				startHeartBeat--;
				return;
			}
			
			AID reciever = new AID(tw.idv.ctfan.cloud.middleware.SystemMonitoringAgent.NAME, AID.ISGUID);
			reciever.addAddresses("http://" + m_masterIP + ":7778/acc");
			heartBeat.addReceiver(reciever);
			
			int waitingJobCount=0;
			int runningJobCount=0;
			int finishedJobCount=0;
			
//			String msg1 = "My AID: " + myAgent.getAID().getName();
//			
//			for(String s:myAgent.getAID().getAddressesArray()) {
//				msg1 += (" " + s);
//			}
//			
//			System.out.println(msg1);
			
			String content ="";
			
			content += (EncodeAgentPositionInfo() + "\n");
			content += (OnEncodeClusterLoadInfo() + "\n");
			
			for(JobListNode jn:m_jobList) {
				content += EncodeJobExcutionInfo(jn) + "\n";
				content += (OnEncodeJobInfo(jn)) + "\n";
				switch(jn.status) {
				case Waiting:
					waitingJobCount++;
					break;
				case Running:
					runningJobCount++;
					break;
				case Finished:
					finishedJobCount++;
				}
			}
			
			heartBeat.setContent(content);
			System.out.println("Sending message");
			myAgent.send(heartBeat);
			System.out.println("Message sent");
//			System.out.println(content);
			
			if(finishedJobCount>0) {
				for(int i=0; i<m_jobList.size(); i++) {
					if(m_jobList.get(i).status==JOB_STATUS.Finished) {
						m_jobList.remove(i);
						i--;
					}
				}
			}
			
			if(waitingJobCount>0) {
				if(runningJobCount<maxExecuteJobNumber){
					if(lastAskExecuteJob<=0){
						JobListNode jn = null;
						for(int i=0; i<m_jobList.size(); i++) {
							jn = m_jobList.get(i);
							if(jn.status == JOB_STATUS.Waiting) break;
							jn = null;
						}
						if(jn!=null) {
							lastAskExecuteJob = 5;
							
							ACLMessage msg = new ACLMessage(ACLMessage.CONFIRM);
							AID aid = new AID(jn.name + "@" + myAgent.getHap(), AID.ISGUID);
							
							msg.addReceiver(aid);
							msg.setContent("START");
							
							myAgent.send(msg);
							jn.executedTime = System.currentTimeMillis();
							jn.status = JOB_STATUS.Running;
						}
					} else {
						lastAskExecuteJob--;
					}
				}
			}	
		}		
	} }

	
	private String EncodeAgentPositionInfo(){
		return ( this.getLocalName() + " " +
					this.here().getName() + " " +
					this.getHap().split(":")[0]);
	}
	
	private String EncodeJobExcutionInfo(JobListNode jn) {
		long currentTime = System.currentTimeMillis();
		String status = (jn.status==JOB_STATUS.Running?"Running":
			(jn.status==JOB_STATUS.Waiting?"Waiting":"Finished"));
		return jn.name + " " + status + " " + (currentTime-jn.lastExist) + " " + 
				(currentTime-jn.executedTime);
	}
	
	
	private class TerminateVMBehaviour extends OneShotBehaviour {
		private static final long serialVersionUID = 4651660479629623002L;
		
		public TerminateVMBehaviour(Agent a) {
			super(a);
		}

		@Override
		public void action() { 
			synchronized(m_jobList) {
				if(m_jobList.size()==0){
					OnTerminateCluster();
					ACLMessage msg = new ACLMessage(ACLMessage.REQUEST);
					AID recv = new AID(tw.idv.ctfan.cloud.middleware.ResourceReconfigurationAgent.name + "@" + m_masterIP + ":1099:/JADE", AID.ISGUID);
					recv.addAddresses("http://" + m_masterIP + ":7778/acc");
					msg.addReceiver(recv);
					
					msg.setContent("TERMINATE VM");
					myAgent.send(msg);					
					
					myAgent.doDelete();
				}
			}
		}		
	}
	
	/**
	 * Encode the cluster's load information.  The information will be sent to {@link SystemMonitoringAgent}
	 * This function is accompany to the {@link JobType#DecodeClusterLoadInfo}
	 * @return
	 */
	protected abstract String OnEncodeClusterLoadInfo();
	
	/**
	 * Encode the job's information.  
	 * This function is accompany to the {@link JobType#UpdateJobNodeInfo}
	 * @param jn
	 * @return
	 */
	protected abstract String OnEncodeJobInfo(JobListNode jn);
	
	/**
	 * This function is called when the request message is being decoded.
	 * All parameters except UID and binaryFile will be decode by this function.
	 * This function is accompany to {@link JobType.OnDispatchJobMsg}
	 * @param jn The job
	 * @param head The parameter name
	 * @param tail The parameter value
	 */
	public abstract void OnDecodeNewJob(JobListNode jn, String head, String tail);
	
	/**
	 * This function returns the command that is used to initialize the {@link JobAgent}
	 * @param jn
	 * @return
	 */
	public abstract String OnEncodeNewJobAgent(JobListNode jn);
	
	/**
	 * This function is used when the cluster is going to be shut.
	 * You can use this function to terminate the environment (Not machine, but the execution environment) properly.
	 */
	public abstract void OnTerminateCluster();
	
	/**
	 * This function should return the name of your agent that extends the {@link JobAgent} .
	 * @return
	 */
	public abstract String GetJobAgentClassName();
	
	/**
	 * This function is used to make sure the cluster is ready to serve.
	 * For example, the environment is ready to perform a job.
	 * @return
	 */
	public abstract boolean InitilizeCluster();
}
