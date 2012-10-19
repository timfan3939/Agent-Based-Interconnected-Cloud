package tw.idv.ctfan.cloud.middleware.MapReduce;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import jade.core.AID;
import jade.core.Agent;
import jade.core.behaviours.Behaviour;
import jade.core.behaviours.CyclicBehaviour;
import jade.core.behaviours.OneShotBehaviour;
import jade.lang.acl.ACLMessage;
import jade.lang.acl.MessageTemplate;

public class JobAgent extends Agent {

	private static final long serialVersionUID = 1L;
	
	private String m_hadoopHome = "";
	private String m_jarHome = "";
	private String m_MRJarName = "";
	private String m_otherParam = "";
	private String m_MPJarParam = "";
	private byte[] m_MPJarFile = null;
	Process m_process = null;
	
	private int mapStatus = 0;
	private int reduceStatus = 0;
	private String MPJobName = "";
	private String masterName = "";
	
	private byte[] buff2k = new byte[0X400];
	private int    bufflen;
	
	private MonitorProcessBehaviour m_MPBehaviour = null;
	private ListeningBehaviour m_ListeningBehaviour = null;
	private HeartBeatBehaviour m_HeartBeat = null;
//	private StartMPBehaviour m_StartMPBehaviour = null;
	
	private FileOutputStream Log;
	
	protected void setup()
	{
		super.setup();
		
		Object[] args = this.getArguments();
		if(args.length < 3)
		{
			System.out.println("Agent Usage: JarHome MapReduceJar masterName [Parameter]*");
			this.doDelete();
			return;
		}
		m_jarHome    = (String)args[0];
		m_MRJarName  = (String)args[1];
		masterName   = (String)args[2];
		m_MPJarParam = (String)args[3];
		m_otherParam = (String)args[4];
		
		
		//m_hadoopHome = "/home/hadoop/hadoop";		
		
		System.out.println(this.getName() + " Load Library Success");	
		
		try {
			Log = new FileOutputStream("/home/hadoop/ctfan/log/" + this.getLocalName() + ".log", false);
		} catch (FileNotFoundException e) {
			System.err.println("Error while opening login file");
			e.printStackTrace();
		}
		
		m_ListeningBehaviour = new ListeningBehaviour(this);
		addBehaviour(m_ListeningBehaviour);

		m_HeartBeat = new HeartBeatBehaviour(this);
		addBehaviour(m_HeartBeat);
	}
	
	private void WriteLog(String s)
	{
		try{
			Log.write(s.getBytes());
			Log.write('\n');
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	
	private class FinishMPBehaviour extends OneShotBehaviour
	{
		private static final long serialVersionUID = 1L;
		public FinishMPBehaviour(Agent agent)
		{
			super(agent);
		}
		
		@Override
		public void action() {
			myAgent.doDelete();
		}
		
	}
	
	private class MonitorProcessBehaviour extends Behaviour
	{
		private static final long serialVersionUID = 1L;
		boolean doneYet = false;
		BufferedInputStream m_BuffInput;
		StringBuffer m_output;
		
		public MonitorProcessBehaviour(Agent agent, Process process)
		{
			super(agent);
			m_process = process;
			m_output = new StringBuffer();
			m_BuffInput = new BufferedInputStream(m_process.getErrorStream());
		}
		
		@Override
		public int onEnd()
		{
			m_process.destroy();
			return 0;
		}

		@Override
		public void action() {
			try{
				if(m_BuffInput.available()>0)
				{
					//System.out.println(myAgent.getName() + ": Stream available");
					
					if( (bufflen = m_BuffInput.read(buff2k)) > 0 )
					{
						//System.out.println(myAgent.getName() + ": Got stream length " + len);
						m_output.append(new String(buff2k, 0, bufflen));
						m_output.append("\n--\n");
					}
					else
					{
						block(5000);
						return;
					}					
					
					String[] subOutput = m_output.toString().split("\n");
					Log.write(m_output.toString().getBytes());

					//System.out.println(myAgent.getName() + " Current result: ");
					for(int i = subOutput.length -1; i >= 0; i--)
					{
						//System.out.println(myAgent.getName() + ": " + i + "\t: " + subOutput[i]);
						String[] subResult = subOutput[i].split(" ");
						if(subOutput[i].contains("Running job"))
						{
							System.out.println(myAgent.getName() + " Job Name: " + subResult[6]);
							MPJobName = subResult[6];
						}
						else if(subOutput[i].contains("map ") && subOutput[i].contains("reduce ") && subOutput[i].contains("%"))
						{
							mapStatus    = Integer.parseInt(subResult[6].substring(0, (subResult[6].length()-1)));
							reduceStatus = Integer.parseInt(subResult[8].substring(0, (subResult[8].length()-1)));
							
							System.out.println(myAgent.getName() + " : Current Status " + mapStatus + "/" + reduceStatus);
							
							if(reduceStatus == 100)
							{
								doneYet = true;
								myAgent.addBehaviour(new FinishMPBehaviour(myAgent));
							}
														
							break;
						}
					}		
				}
				
				block(1000);
			
			} catch (Exception e)
			{
				e.printStackTrace();
				doneYet = true;
			}
		}

		@Override
		public boolean done() {
			return doneYet;
		}		
	}
	
	
	private class MoveBehaviour extends Behaviour
	{
		private static final long serialVersionUID = 1L;
		boolean doneYet = false;
		
		String d_agentName;
		String d_address;
		//String d_container;
		
		public MoveBehaviour(Agent agent, String info)
		{
			super(agent);
		
				
			String[] subInfo = info.split(" ");
			d_agentName = subInfo[1];
			//d_container = subInfo[2];
			d_address = subInfo[3];
			
			System.out.println(myAgent.getName() + " Move Behaviour");
			
			try{				
				System.out.println(myAgent.getName() + ": Start Loading File");
				File file = new File(m_jarHome + "/" + m_MRJarName);
				FileInputStream fin = new FileInputStream(file);
				ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
				
				while( (bufflen = fin.read(buff2k)) > 0)
				{
					outputStream.write(buff2k, 0, bufflen);
				}
				fin.close();
				m_MPJarFile = outputStream.toByteArray();
				System.out.println(myAgent.getName() + ": Done Loading File");
				
				
			} catch(Exception e)
			{
				e.printStackTrace();
			}
		}
		
		@Override
		public void action() {			
			try {

				WriteLog("Sending Message");
				ACLMessage msg = new ACLMessage(ACLMessage.REQUEST);
				
				AID reciever = new AID(d_agentName + "@" + d_address + ":1099/JADE", AID.ISGUID);
				reciever.addAddresses("http://" + d_address + ":7778/acc");
				msg.addReceiver(reciever);
				
				String param = m_otherParam +
					 		   "Parameter:" + m_MPJarParam + "\n" +
				               "BinaryDataLength:" + m_MPJarFile.length + "\n";
				
				ByteArrayOutputStream s = new ByteArrayOutputStream();
				s.write(param.getBytes());
				s.write(m_MPJarFile);
				
				msg.setByteSequenceContent(s.toByteArray());
				s.close();
				
				
				myAgent.send(msg);			
				
				doneYet = true;
				
			} catch(Exception e)
			{
				doneYet = true;
				e.printStackTrace();
				return;
			}						
		}

		@Override
		public boolean done() {
			return doneYet;
		}		
	}
	
	private class RemoveAgentBehaviour extends Behaviour
	{
		private static final long serialVersionUID = 1L;
		MoveBehaviour m_moveB;
		
		boolean doneYet = false;
		RemoveAgentBehaviour(Agent agent, String msg)
		{
			super(agent);
			
			m_moveB = new MoveBehaviour(agent, msg);
			
			agent.addBehaviour(m_moveB);
			
			if(m_MPBehaviour!=null)
				agent.removeBehaviour(m_MPBehaviour);
		}

		@Override
		public void action() {
			WriteLog("Try to remove agent");
			if(m_moveB.done())
			{
				doneYet = true;
				myAgent.doDelete();
			}
			else
				doneYet = false;
		}

		@Override
		public boolean done() {
			return doneYet;
		}
		
	}
	
	private class ListeningBehaviour extends CyclicBehaviour
	{ 
		private static final long serialVersionUID = 1L;
		
		public ListeningBehaviour(Agent agent)
		{
			super(agent);
		}

		@Override
		public void action() {
			try {
			
				MessageTemplate mt = MessageTemplate.MatchAll();
				ACLMessage msg = myAgent.receive(mt);
				if(msg == null)
				{
					block();
					return;
				}
				
				System.out.println(myAgent.getName() + " Got Message from " + msg.getSender().getName());
				
				switch(msg.getPerformative())
				{
					case ACLMessage.INFORM:
						String info = msg.getContent();
						Log.write("Got Migrate Message".getBytes());
						if(info.startsWith("suggest"))
						{
							System.out.println("Got Migrate Message");
							//if( !MPJobName.isEmpty())
							{															
								ACLMessage msg001 = new ACLMessage(ACLMessage.INFORM);
								msg001.addReceiver(new AID(masterName + "@" + myAgent.getAID().getHap(), AID.ISGUID));
								msg001.setContent("Migration");
								myAgent.send(msg001);
							}
							myAgent.addBehaviour(new RemoveAgentBehaviour(myAgent, msg.getContent()));	
						}
						break;
					case ACLMessage.CONFIRM:
						if(m_hadoopHome == null || m_hadoopHome.isEmpty())
						{
							m_hadoopHome = msg.getContent();
							
							Process p;
							String command = m_hadoopHome + "/bin/hadoop jar " +
							                 m_jarHome + "/" + m_MRJarName + " " +
							                 m_MPJarParam;
							//System.out.println(myAgent.getName() + ": " + command);
							Runtime rt = Runtime.getRuntime();
							p = rt.exec(command);
							
							m_MPBehaviour = new MonitorProcessBehaviour(myAgent, p);
							myAgent.addBehaviour(m_MPBehaviour);
						}
						break;
					default:
						//myAgent.putBack(msg);
						break;
				}
			} catch(Exception e)
			{
				e.printStackTrace();
			}
		}		
	}
	
	private class HeartBeatBehaviour extends Behaviour
	{
		private static final long serialVersionUID = 1L;
		boolean doneYet = false;
		
		public HeartBeatBehaviour(Agent agent)
		{
			super(agent);
		}

		@Override
		public void action() {
			ACLMessage msg = new ACLMessage(ACLMessage.CONFIRM);
			msg.addReceiver(new AID(masterName + "@" + myAgent.getHap(), AID.ISGUID));
			
			msg.setContent(MPJobName);
			
			//System.out.println(myAgent.getName() + " heartbeat " + MPJobName + ";");
									
			myAgent.send(msg);
			
			block(3000);
			
		}

		@Override
		public boolean done() {
			return doneYet;
		}
		
	}
	
}
