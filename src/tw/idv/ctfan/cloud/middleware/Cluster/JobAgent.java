package tw.idv.ctfan.cloud.middleware.Cluster;

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
		
		addBehaviour(new HeartBeatBehaviour(this, 3000));
		addBehaviour(new ListeningBehaviour(this));
	}
	
	protected String getMasterName() {
		return masterName;
	}

	protected void setMasterName(String masterName) {
		this.masterName = masterName;
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
					// TODO: add migrate procedure here
				} break;
				case ACLMessage.CONFIRM: {
					WriteLog("Got Start Job Message");
					if(!startedYet) {
						StartJob();
						startedYet = true;
						finishedYet = false;
					} else if(startedYet && !finishedYet) {
						System.err.println("Job has been started, Duplicated message");
					} else {
						myAgent.doDelete();
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
	
	protected abstract void StartJob();
	
	protected abstract String OnHeartBeat();

}
