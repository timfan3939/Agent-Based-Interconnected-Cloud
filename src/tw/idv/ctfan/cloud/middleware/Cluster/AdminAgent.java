package tw.idv.ctfan.cloud.middleware.Cluster;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;

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
	
	protected String m_jarPath;
	protected String m_masterIP;
	
	protected int maxExecuteJobNumber = 1;
	
	private boolean firstHeartBeat = false;

	protected enum JOB_STATUS {
		Waiting, Running, Finished;
	}
	
	protected class JobListNode {
		public String name;
		public long lastExist;
		public int hasBeenExecute;
		public long executedTime = 0;
		JOB_STATUS status;
		public ArrayList<String> cmdParam;
		public byte[] binaryFile;
		
		public JobListNode(String name, ArrayList<String> cmd, byte[] bin) {
			this.name = name;
			this.cmdParam = cmd;
			this.binaryFile = bin;
			lastExist = -1;
			hasBeenExecute = -1;
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
		
		this.addBehaviour(new MessageListeningBehaviour(this));
		this.addBehaviour(new HeartBeatBehaviour(this, 3000));
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
				newJob = OnDecodeNewJob(m_data);
				hasParsed = true;
			}
			if(newJob==null) return;
			synchronized(m_jobList) {
				try {
					File f = new File(m_jarPath + "/" + newJob.name + m_jobType.GetExtension());
					if(!f.exists()) {
						FileOutputStream output = new FileOutputStream(f);
						output.write(newJob.binaryFile);
						output.close();
						newJob.binaryFile = null;
					}
					
					m_jobList.add(newJob);

					newJob.cmdParam.add(0, myAgent.getLocalName());
					newJob.cmdParam.add(1, m_jarPath);
					newJob.cmdParam.add(2, newJob.name+m_jobType.GetExtension());
					
					myAgent.getContainerController().createNewAgent(newJob.name, tw.idv.ctfan.cloud.middleware.Java.JavaJobAgent.class.getName(), newJob.cmdParam.toArray()).start();
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
		@Override
		protected void onTick(){ synchronized(m_jobList) {
			ACLMessage heartBeat = new ACLMessage(ACLMessage.CONFIRM);
			
			if(!firstHeartBeat) {
				heartBeat.setPerformative(ACLMessage.REQUEST);
			}
			
			AID reciever = new AID(tw.idv.ctfan.cloud.middleware.SystemMonitoringAgent.NAME, AID.ISGUID);
			reciever.addAddresses("http://" + m_masterIP + ":7778/acc");
			heartBeat.addReceiver(reciever);
			
			int waitingJobCount=0;
			int runningJobCount=0;
			int finishedJobCount=0;
			
			String msg1 = "My AID: " + myAgent.getAID().getName();
			
			for(String s:myAgent.getAID().getAddressesArray()) {
				msg1 += (" " + s);
			}
			
			System.out.println(msg1);
			
			String content = "cluster " +
				myAgent.getLocalName() + " " +
				myAgent.here().getName() + " " +
				myAgent.getHap().split(":")[0] + "\n";
			content += "Load:" + OnEncodeLoadInfo() + "\n";
			for(JobListNode jn:m_jobList) {
				content += "job " + OnEncodeJobInfo(jn) + "\n";
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
							lastAskExecuteJob = 10;
							
							ACLMessage msg = new ACLMessage(ACLMessage.CONFIRM);
							AID aid = new AID(jn.name + "@" + myAgent.getHap(), AID.ISGUID);
							
							msg.addReceiver(aid);
							msg.setContent("START");
							
							myAgent.send(msg);
							jn.executedTime = System.currentTimeMillis();
						}
					} else {
						lastAskExecuteJob--;
					}
				}
			}	
		}		
	} }
	
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
	
	protected abstract String OnEncodeLoadInfo();
	protected abstract String OnEncodeJobInfo(JobListNode jn);		
	public abstract JobListNode OnDecodeNewJob(byte[] data);
	public abstract void OnTerminateCluster();
}
