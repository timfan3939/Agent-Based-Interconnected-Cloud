package tw.idv.ctfan.cloud.middleware;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;

import jade.core.Agent;
import jade.core.behaviours.Behaviour;
import jade.core.behaviours.CyclicBehaviour;
import jade.core.behaviours.TickerBehaviour;
import jade.lang.acl.ACLMessage;
import jade.lang.acl.MessageTemplate;
import jade.wrapper.StaleProxyException;

public abstract class ClusterAdminAgent extends Agent {

	/**
	 * What, this is just a serial version UID, nothing special
	 */
	private static final long serialVersionUID = 1L;
	
	protected String m_jarPath;
	protected String m_masterIP;
	
	protected int maxExecuteJobNumber = 1;
	

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
		
	public void setup() {
		super.setup();		
		
		this.addBehaviour(new MessageListeningBehaviour(this));
		this.addBehaviour(new HeartBeatBehaviour(this, 3000));
	}

	protected String getM_jarPath() {
		return m_jarPath;
	}

	protected void setM_jarPath(String mJarPath) {
		m_jarPath = mJarPath;
	}

	protected String getM_masterIP() {
		return m_masterIP;
	}

	protected void setM_masterIP(String mMasterIP) {
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
						// TODO: add some terminate behaviour here
					}
				} break;
				case ACLMessage.PROPOSE:
				case ACLMessage.CONFIRM: {
					for(JobListNode jn:m_jobList) {
						if(jn.name.compareTo(msg.getSender().getLocalName()) == 0) {
							jn.SetExist();
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
			try {
				File f = new File(m_jarPath + "/" + newJob.name + ".jar");
				if(!f.exists()) {
					FileOutputStream output = new FileOutputStream(f);
					output.write(newJob.binaryFile);
					output.close();
					newJob.binaryFile = null;
				}
				
				m_jobList.add(newJob);
				// TODO: change the class name to be more general
				myAgent.getContainerController().createNewAgent(newJob.name, tw.idv.ctfan.cloud.middleware.Java.JobAgent.class.getName(), newJob.cmdParam.toArray()).start();
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

		@Override
		public boolean done() {
			return doneYet;
		}		
	}
	
	public abstract JobListNode OnDecodeNewJob(byte[] data);
	
	private class HeartBeatBehaviour extends TickerBehaviour {
		private static final long serialVersionUID = 1L;
		
		private long lastAskExecuteJob;

		public HeartBeatBehaviour(ClusterAdminAgent agent, int i) {
			super(agent, i);
			lastAskExecuteJob = 0;
		}

		@Override
		protected void onTick() {
			// TODO Auto-generated method stub
			
		}
		
	}
}
