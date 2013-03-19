package tw.idv.ctfan.cloud.middleware.Java;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
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

public class ClusterAdminAgent extends Agent {

	private static final long serialVersionUID = 1L;
	
	private String m_jarPath;
	private String m_masterIP;
	
	private static int maxExecuteJobNumber = 1;
	
	private ArrayList<JobListNode> m_jobList = new ArrayList<JobListNode>();
	
	public void setup() {
		super.setup();
		
		Object[] args = this.getArguments();
		if(args.length != 2) {
			System.err.println("Usage: JarHome MasterIP");
			this.doDelete();
		}
			
		m_jarPath = (String)args[0];
		m_masterIP= (String)args[1];
		
		System.out.println("Jar    Path: " + m_jarPath + ":");
		System.out.println("Master IP  : " + m_masterIP+ ":");
		
		this.addBehaviour(new MessageListeningBehaviour(this));
		this.addBehaviour(new HeartBeatBehaviour(this, 3000));
	}
	
	private class MessageListeningBehaviour extends CyclicBehaviour {
		private static final long serialVersionUID = 1L;

		/*
		 * Request	: new job
		 * Propose	: job Execution (but have to wait)
		 * Inform	: job execution/migration
		 * Confirm	: job exists 
		 */
		public MessageListeningBehaviour(Agent agent) {
			super(agent);
		}

		@Override
		public void action() {
			try {
				MessageTemplate mt = MessageTemplate.MatchAll();
				ACLMessage msg = myAgent.receive(mt);
				JobListNode JNode;
				
				if(msg==null) {
					block();
					return;
				}
				
				switch(msg.getPerformative()) {
					case ACLMessage.REQUEST: {
						byte[] program = msg.getByteSequenceContent();
						
						myAgent.addBehaviour(new NewJobBehaviour(myAgent, program));
					}
					break;
					case ACLMessage.INFORM: {
						String info = msg.getContent();
						if(info.matches("MIGRATION")) {
							for(int i=0; i<m_jobList.size(); i++) {
								JNode = m_jobList.get(i);
								if(JNode.name.compareTo(msg.getSender().getLocalName())==0) {
									myAgent.addBehaviour(new TerminateJobBehaviour(myAgent, JNode));
									m_jobList.remove(i);
									break;
								}
							}
						} 
						else if(info.matches("TERMINATE")){
							myAgent.addBehaviour(new TerminateVMBehaviour(myAgent));
						}
					}
					case ACLMessage.PROPOSE:
					case ACLMessage.CONFIRM:{
						JNode = null;
						for(int i=0; i<m_jobList.size(); i++) {
							JNode = m_jobList.get(i);
							if(JNode.name.compareTo(msg.getSender().getLocalName())==0) {
								JNode.SetExist();
								break;
							}
						}
					}
					break;
				}
				
			} catch(Exception e) {
				e.printStackTrace();
			}
			
		}
		
	}
	
	private class TerminateVMBehaviour extends OneShotBehaviour {
		private static final long serialVersionUID = 1L;
		
		public TerminateVMBehaviour(Agent a){
			super(a);
		}

		@Override
		public void action() {			
			if(m_jobList.size()==0) {
				System.out.println("This Agent and Container will be terminated");				
				
				try {
					myAgent.getContainerController().kill();
				} catch (StaleProxyException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}
		
	}
	
	private class TerminateJobBehaviour extends OneShotBehaviour {
		
		private static final long serialVersionUID = 1L;
		JobListNode jn;
		
		public TerminateJobBehaviour(Agent agent, JobListNode jNode) {
			super(agent);
			this.jn = jNode;
		}

		@Override
		public void action() {	
		}		
	}
	
	private class NewJobBehaviour extends Behaviour {
		private static final long serialVersionUID = 1L;
		byte[] m_data;
		boolean hasParced;
		
		// Job's properties
		String		jobUID;
		int 		jobLength;
		String		jobParameter;
		byte[]		jobBinaryFile;
		boolean		doneYet = false;
		
		public NewJobBehaviour(Agent agent, byte[] program) {
			super(agent);
			
			m_data = program.clone();
			
			hasParced = false;
		}
		
		@Override
		public void action() {
			int c = 0;
			int buffLen;
			byte[] buff = new byte[0x400];
			String infoToJobAgent = "";
			
			if(!hasParced) {
				ByteArrayInputStream dataInput = new ByteArrayInputStream(m_data);
				
				while(dataInput.available()>0) {
					buffLen = 0;
					while( (c = dataInput.read()) != '\n') {
						buff[buffLen] = (byte)c;
						buffLen++;
					}
					String line = new String(buff, 0, buffLen);
					String head = line.substring(0, line.indexOf(":"));
					String tail = line.substring(line.indexOf(":")+1);
					
					if(head.matches("UID")) {
						jobUID = tail;
						infoToJobAgent += ("UID:" + jobUID + "\n");
					}
					else if(head.matches("BinaryDataLength")) {
						try {
							jobLength = Integer.parseInt(tail);
							jobBinaryFile = new byte[jobLength];
							dataInput.read(jobBinaryFile, 0, jobLength);
						} catch(Exception e) {
							System.err.println("Original Line:" + line);
							e.printStackTrace();
							return;
						}
					}
					else if(head.matches("Parameter")) {
						jobParameter = tail;
					}
				}
				hasParced = true;
			}			
			try {
				File f = new File(m_jarPath + "/" + "job" + jobUID + ".jar");
				
				if(!f.exists()) {
					FileOutputStream output = new FileOutputStream(m_jarPath + "/" + "job" + jobUID + ".jar");
					output.write(jobBinaryFile);
					output.close();
				}
				
				ArrayList<String> parameterList = new ArrayList<String>();
				parameterList.add(m_jarPath);
				parameterList.add("job" + jobUID + ".jar");
				parameterList.add(myAgent.getLocalName());
				parameterList.add(jobParameter);
				parameterList.add(infoToJobAgent);
				
				JobListNode jn = new JobListNode("job" + jobUID);
				m_jobList.add(jn);
				myAgent.getContainerController().createNewAgent(jn.name, tw.idv.ctfan.cloud.middleware.Java.JobAgent.class.getName(), parameterList.toArray()).start();
				System.out.println("=====Agent Start=====");
				doneYet = true;
				
			} catch(StaleProxyException e) {
				System.err.println("===Agent Exception===");
				e.printStackTrace();
				block(5000);
				return;
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}
		
		@Override
		public boolean done() {
			return doneYet;
		}
		
	}
	
	private class JobListNode {
		public String name;
		public long lastExist;
		public int hasBeenExecute;
		public int status;
		public long executedTime = 0;
		
		public static final int WAITING = 1;
		public static final int RUNNING = 2;
		public static final int FINISHED = 3;
		
		public JobListNode(String name) {
			this.name = name;
			lastExist = -1;
			hasBeenExecute = -1;
			status = WAITING;
		}
		
		public void SetExist() {
			lastExist = System.currentTimeMillis();
		}
	}
	
	private class HeartBeatBehaviour extends TickerBehaviour {
		
		private static final long serialVersionUID = 1L;
		private int lastAskExecuteJob;

		public HeartBeatBehaviour(Agent a, long period) {
			super(a, period);
			lastAskExecuteJob = 0;
		}
		
		private String[] GetAllJavaJob() {
			try {
				Runtime rt = Runtime.getRuntime();
				Process p = rt.exec("jps");
				BufferedInputStream buffer = new BufferedInputStream(p.getInputStream());
				p.waitFor();
				
				byte[] buff = new byte[0x400];
				int read = 0;
				String result = "";
				while((read = buffer.read(buff))>0) {
					result += new String(buff, 0, read);
				}
				if(!result.isEmpty()) {
					String[] subResult = result.split("\n");
					for(int i=0; i<subResult.length; i++){
						subResult[i] = subResult[i].substring(subResult[i].indexOf(" ")+1);
						System.out.println("" + i + ":\t" + subResult[i]);
					}
					
					return subResult;
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
			String[] nothing = new String[0];
			return nothing;
		}

		@Override
		protected void onTick() {
			ACLMessage heartBeat = new ACLMessage(ACLMessage.CONFIRM);
			
			System.out.println("=== This Message ===");
			
			AID reciever = new AID("CloudAdmin@" + m_masterIP + ":1099/JADE", AID.ISGUID);
			reciever.addAddresses("http://" + m_masterIP + ":7778/acc");
			heartBeat.addReceiver(reciever);
			
			String content = "cluster ";
			
			// send who am I
			content += myAgent.getLocalName() + " " 
					 + myAgent.here().getName() + " "
					 + myAgent.getHap().split(":")[0] + "\n";
			
			// send System load
			content += ("load " + -1 + "\n");
			
			// send Job Information
			int runningSpaceLeft = maxExecuteJobNumber;
			long currentTime = System.currentTimeMillis();
			String[] jobs = this.GetAllJavaJob();
			
			for(int i=0; i<m_jobList.size(); i++) {
				JobListNode JBNode = m_jobList.get(i);
				if(JBNode.status == JobListNode.WAITING) {
					for(int j=0; j<jobs.length; j++) {
//						System.out.println(jobs[j] + "\t<->\t" + JBNode.name);
						if(jobs[j].matches(JBNode.name+".jar")) {
							JBNode.status = JobListNode.RUNNING;
							break;
						}
					}
					if(JBNode.status == JobListNode.WAITING) {
						content += ("job java " + JBNode.name + " waiting " + (currentTime-JBNode.lastExist)) + "\n";
					}
					else if(JBNode.status == JobListNode.RUNNING) {
						content += ("job java " + JBNode.name + " running " + (currentTime-JBNode.lastExist)) + " " + (currentTime-JBNode.executedTime) + "\n";
						runningSpaceLeft--;
					}
				}
				else if(JBNode.status == JobListNode.RUNNING) {
					boolean found = false;
					for(int jobCount=0; jobCount<jobs.length; jobCount++) {
						if(jobs[jobCount].matches(JBNode.name+".jar"))
							found = true;
					}
					
					if(found) {
						content += ("job java " + JBNode.name + " running " + (currentTime-JBNode.lastExist)) + " " + (currentTime-JBNode.executedTime) + "\n";
						runningSpaceLeft--;
					}
					else {
						content += ("job java " + JBNode.name + " finished " + (currentTime-JBNode.executedTime)) + "\n";
						m_jobList.remove(i);
						i--;
					}
				}
				else if (JBNode.status == JobListNode.FINISHED) {
					content += ("job java " + JBNode.name + " finished " + (currentTime-JBNode.executedTime)) + "\n"; // TODO: add finish time
				}
			}
			System.out.println(content + "\n");
			heartBeat.setContent(content);
			myAgent.send(heartBeat);	
			
			System.out.println("Running Space Left:\t" + runningSpaceLeft + "\tLastAskExecuteJob:\t" + lastAskExecuteJob);
			
			
			if( (runningSpaceLeft>0 && m_jobList.size()>0 && lastAskExecuteJob<=0) ||
				(runningSpaceLeft==maxExecuteJobNumber && m_jobList.size()>0)) {
				
				JobListNode jn = null;
				for(int i=0; i<m_jobList.size(); i++) {
					JobListNode curr = m_jobList.get(i);
					if(curr.hasBeenExecute>0) {
						curr.hasBeenExecute--;
						curr=null;
					}
					else if(curr.hasBeenExecute==0) {
						curr.hasBeenExecute = -1;
					}
					
					if(curr!=null && curr.hasBeenExecute == -1) {
						if(jn!=null){
							if(jn.name.compareTo(curr.name)>0) {
								jn = curr;
							}
						}
						else
							jn = curr;
					}
				}

				if(jn != null) {
					String jobName = jn.name;
					jn.hasBeenExecute = 30;
					
					
					System.out.println("Start New Job");
					ACLMessage msg = new ACLMessage(ACLMessage.CONFIRM);
					AID aid = new AID(jobName + "@" + myAgent.getHap(), AID.ISGUID);
					
					msg.addReceiver(aid);
					msg.setContent("start");
					
					myAgent.send(msg);
					jn.executedTime = System.currentTimeMillis();
					lastAskExecuteJob = 10;
				}
			}			
			else
				lastAskExecuteJob--;
		}
	}
}
