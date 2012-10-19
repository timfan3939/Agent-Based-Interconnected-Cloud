package tw.idv.ctfan.cloud.middleware.Java;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import jade.core.AID;
import jade.core.Agent;
import jade.core.behaviours.Behaviour;
import jade.core.behaviours.CyclicBehaviour;
import jade.core.behaviours.TickerBehaviour;
import jade.lang.acl.ACLMessage;
import jade.lang.acl.MessageTemplate;

public class JobAgent extends Agent {
	private static final long serialVersionUID = 1L;
	
	private String m_jarHome = "";
	private String m_JarName = "";
	private String m_otherParam = "";  // used when migration
	private String m_JarParam = "";
	private byte[] m_JarFile = null;   // used when migration
	private Process m_process = null;
	
	private String masterName = "";
	
	private byte[] buff2k = new byte[0X400];
	private int bufflen;
	
	private MonitorProcessBehaviour m_JavaBehaviour = null;
	private ListeningBehaviour m_ListeningBehaviour = null;
	private HeartBeatBehaviour m_HeartBeat = null;
	
	private FileOutputStream Log;
	
	protected void setup() {
		super.setup();
		
		Object[] args = this.getArguments();
		if(args.length < 3) {
			System.out.println("Agent Usage: JarHome JobJar masterName [Parameter]*");
			this.doDelete();
			return;
		}
		
		m_jarHome		= (String)args[0];
		m_JarName		= (String)args[1];
		masterName		= (String)args[2];
		m_JarParam		= (String)args[3];
		m_otherParam	= (String)args[4];
		
		System.out.println(this.getName() + " Load Livrary Success");
		
		try {
			Log = new FileOutputStream("/home/hadoop/ctfan/log/" + this.getLocalName() + ".log", false);
		} catch (FileNotFoundException e) {
			System.err.println("Error while opening login file");
			e.printStackTrace();
		}
		
		m_ListeningBehaviour = new ListeningBehaviour(this);
		addBehaviour(m_ListeningBehaviour);
		
		m_HeartBeat = new HeartBeatBehaviour(this, 3000);
		addBehaviour(m_HeartBeat);
	}
	
	private void WriteLog(String s) {
		try {
			Log.write(s.getBytes());
			Log.write('\n');
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private class MonitorProcessBehaviour extends Behaviour {
		private static final long serialVersionUID = 1L;
		boolean doneYet = false;
		BufferedInputStream m_BuffInput;
		StringBuffer m_output;
		
		public MonitorProcessBehaviour(Agent agent, Process process) {
			super(agent);
			m_process = process;
			m_output = new StringBuffer();
			m_BuffInput = new BufferedInputStream(m_process.getErrorStream());
		}

		@Override
		public void action() {
			try {
				if(m_BuffInput.available()>0) {
					if( (bufflen = m_BuffInput.read(buff2k)) > 0) {
						m_output.append(new String(buff2k, 0, bufflen));
						m_output.append("\n--\n");
					}
					else {
						block(5000);
						return;
					}
				}
				WriteLog(m_output.toString());
				
				try {
					m_process.exitValue();
				} catch (IllegalThreadStateException e) {
					System.out.println("Not Filished Yet");
					block(5000);
					return;
				}				
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			doneYet = true;
			myAgent.doDelete();
		}

		@Override
		public boolean done() {
			return doneYet;
		}
	}
	
	private class ListeningBehaviour extends CyclicBehaviour {
		private static final long serialVersionUID = 1L;
		
		public ListeningBehaviour(Agent a) {
			super.setAgent(a);
		}

		@Override
		public void action() {
			try {
				MessageTemplate mt = MessageTemplate.MatchAll();
				ACLMessage msg = myAgent.receive(mt);
				if(msg==null) {
					block();
					return;
				}
				
				System.out.println(myAgent.getName() + " Got Message from " + msg.getSender().getName());
				
				switch(msg.getPerformative()) {
				case ACLMessage.INFORM:
					String info = msg.getContent();
					WriteLog("Got Migrate Message");
					System.out.println("No Migration Available: " + info);
					break;
				case ACLMessage.CONFIRM:
					String command = "java -jar " + m_jarHome + "/" + m_JarName + " " + m_JarParam;
					System.out.println(myAgent.getName() + ": " + command + ":_:");
					WriteLog(command);
					Runtime rt = Runtime.getRuntime();
					Process p = rt.exec(command);
					
					m_JavaBehaviour = new MonitorProcessBehaviour(myAgent, p);
					myAgent.addBehaviour(m_JavaBehaviour);
					break;
				default:
					break;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
	}
	
	private class HeartBeatBehaviour extends TickerBehaviour {

		private static final long serialVersionUID = 1L;
		
		public HeartBeatBehaviour(Agent agent, long period) {
			super(agent, period);
		}
		
		@Override
		protected void onTick() {
			ACLMessage msg = new ACLMessage(ACLMessage.CONFIRM);
			msg.addReceiver(new AID(masterName + "@" + myAgent.getHap(), AID.ISGUID));
			msg.setContent("Hello there");
			
			myAgent.send(msg);			
		}		
	}
}
