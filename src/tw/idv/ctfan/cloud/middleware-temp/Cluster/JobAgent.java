package tw.idv.ctfan.cloud.Middleware.Cluster;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import jade.core.AID;
import jade.core.Agent;
import jade.core.behaviours.CyclicBehaviour;
import jade.core.behaviours.TickerBehaviour;
import jade.lang.acl.ACLMessage;
import jade.lang.acl.MessageTemplate;

public abstract class JobAgent extends Agent {

	private static final long serialVersionUID = 1L;
	protected String masterName;
	boolean startedYet = false;
	boolean finishedYet= false;
	
	protected Object[] parameter;
	protected String m_binaryHome;
	protected String m_binaryName;
	protected String m_otherInfo;
	
	private FileOutputStream Log;
	
	protected void WriteLog(String s){
		try {
			Log.write(s.getBytes());
			Log.write('\n');
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	protected void setup() {
		try {
			Log = new FileOutputStream("/home/hadoop/ctfan/log/" + getLocalName() + ".log", false);
		} catch (FileNotFoundException e) {
			System.err.println("Error while opeing login file");
			e.printStackTrace();
		}
		
		parameter = this.getArguments();
		
		masterName = (String)parameter[0];
		m_binaryHome = (String)parameter[1];
		m_binaryName = (String)parameter[2];
		m_otherInfo = (String)parameter[3];
		
		System.out.println(masterName + "\t" + m_binaryHome + "\t" + m_binaryName + "\t" + m_otherInfo);
		
		addBehaviour(new HeartBeatBehaviour(this, 3000));
		addBehaviour(new ListeningBehaviour(this));
	}
	
	protected String getMasterName() {
		return masterName;
	}

	protected void setMasterName(String masterName) {
		this.masterName = masterName;
	}
	
	protected String GetBinaryFullPath() {
		return m_binaryHome + "/" + m_binaryName;
	}

	private class HeartBeatBehaviour extends TickerBehaviour {
	private static final long serialVersionUID = 1L;

		public HeartBeatBehaviour(Agent a, int t) {
			super(a, t);
		}

		@Override
		protected void onTick() {
			ACLMessage msg = new ACLMessage(ACLMessage.CONFIRM);
			msg.addReceiver(new AID(masterName + "@" + myAgent.getHap(), AID.ISGUID));
			if(!startedYet) {
				msg.setContent("WAITING");
			} else if(startedYet && !finishedYet) {
				msg.setContent(OnHeartBeat());
			} else {
				msg.setContent("FINISHED");
			}
			
			myAgent.send(msg);		
			if(finishedYet) {
				myAgent.doDelete();
			}
		}
	}
	
	private class ListeningBehaviour extends CyclicBehaviour {
		private static final long serialVersionUID = 1L;

		public ListeningBehaviour(Agent a) {
			super(a);
		}
		
		@Override
		public void action() {
			try {
				ACLMessage msg = myAgent.receive(MessageTemplate.MatchAll());
				if(msg == null) {
					block();
					return;
				}
				
				switch(msg.getPerformative()) {
				case ACLMessage.INFORM:{
					WriteLog("Got Migrate Message");
					// TODO: migrate procedure 
				} break;
				case ACLMessage.CONFIRM: {
					WriteLog("Got Start Job Message");
					if(!startedYet) {
						StartJob(myAgent, m_otherInfo);
						startedYet = true;
						finishedYet = false;
					} else if(startedYet && !finishedYet) {
						System.err.println("Job has been started, Duplicated message");
					} else {
						//myAgent.doDelete();
						// TODO: not knowing what to do here
					}
					
				} break;
				}
				
			} catch(Exception e) {
				e.printStackTrace();
			}			
		}		
	}
	
	protected void JobFinished() {
		finishedYet = true;
	}
	
	/**
	 * Start a job
	 * @param myAgent
	 * @param info
	 */
	protected abstract void StartJob(Agent myAgent, String info);
	
	/**
	 * Return jobAgent info to {@link AdminAgent}
	 * @return
	 */
	protected abstract String OnHeartBeat();

}
