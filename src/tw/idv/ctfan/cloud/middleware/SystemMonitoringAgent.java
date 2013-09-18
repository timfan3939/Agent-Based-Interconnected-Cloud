package tw.idv.ctfan.cloud.middleware;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

import tw.idv.ctfan.cloud.middleware.Cluster.JobType;
import tw.idv.ctfan.cloud.middleware.policy.*;
import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNode;
import jade.core.Agent;
import jade.core.behaviours.Behaviour;
import jade.core.behaviours.CyclicBehaviour;
import jade.core.behaviours.OneShotBehaviour;
import jade.core.behaviours.ThreadedBehaviourFactory;
import jade.lang.acl.ACLMessage;
import jade.lang.acl.MessageTemplate;

public class SystemMonitoringAgent extends Agent {
	
	ThreadedBehaviourFactory tbf;	
	Policy policy;
	private static final long serialVersionUID = 1L;	
	
	private final String fileDirectory = "C:\\ctfan\\middlewareFile\\";
	
	public static final String NAME = "SyMA@120.126.145.102:1099/JADE";
	
	public void setup() {
		super.setup();
		
		tbf = new ThreadedBehaviourFactory();	
		
		policy = MultiTypePolicy.GetPolicy();
				
		this.addBehaviour(tbf.wrap(new SubmitBehaviour(this) ) );
		this.addBehaviour(tbf.wrap(new HTTPServerBehaviour(this, policy) ) );
		this.addBehaviour(tbf.wrap(new ListeningBehaviour(this) ) );
	}
	
	public void AddTbfBehaviour(Behaviour b) {
		this.addBehaviour(this.tbf.wrap(b));
	}
	
	/**
	 * Quick submit behaviour
	 * @author C.T.Fan
	 *
	 */
	private class SubmitBehaviour extends CyclicBehaviour {
		private static final long serialVersionUID = 1L;
		ServerSocket server;
		
		public SubmitBehaviour(SystemMonitoringAgent agent) {
			super(agent);
			try {
				server = new ServerSocket(50031);				
			} catch(Exception e){
				System.err.println("Creating socket error");
				e.printStackTrace();
				myAgent.doDelete();
			}
		}

		@Override
		public void action() {
			try {
				Socket s = server.accept();
				
				System.out.println(myAgent.getLocalName() + ": Got Client");
				
//				String host = s.getInetAddress().getHostAddress();
				InputStream input = s.getInputStream();
				
				JobNode jn = new JobNode();
				byte[] jobBinaryFile = null;
				
				String line = "";
				String head = "";
				String tail = "";				
				
				int ch;
				
				byte[] buff = new byte[0x1000];
				int bufflen = 0;
				
				while(true) {
					
					bufflen = 0;
					while( (ch = input.read()) >=0 && ch != '\n' ) {
						buff[bufflen] = (byte)ch;
						bufflen++;
					}
					if(ch < 0) break;
					
					
					if(ch!='\n') continue;
					line = new String(buff, 0, bufflen);
//					System.out.println(line);
					head = line.substring(0, line.indexOf(':'));
					tail = line.substring(line.indexOf(':')+1);
					
					if(head.matches("BinaryDataLength")) {
						int jobLength = Integer.parseInt(tail);
						jobBinaryFile = new byte[jobLength];
						int read = input.read(jobBinaryFile, 0, jobLength);
						
						while(read<jobLength && (ch = input.read(buff)) > 0) {
							for(int i=0; i<ch; i++) {
								jobBinaryFile[read+i] = buff[i];
							}
							read += ch;
						}
					} else if(head.matches("Deadline")) {
						jn.deadline = Long.parseLong(tail);
						bufflen = 0;
					} else if(!head.isEmpty()&&!tail.isEmpty()){
						jn.AddDiscreteAttribute(head, tail);
					}
				}
				
				s.close();
				
					
				if(jobBinaryFile!=null) {
					myAgent.addBehaviour(tbf.wrap(new GetJobInfoBehaviour(myAgent, jn, jobBinaryFile)));
				}				
				
				buff = null;							
			} catch( Exception e ) {
				e.printStackTrace();
			}
		}		
	}
	
	public void SubmitJob(JobNode newJob, byte[] binaryFile) {
		// Method for HTTPServerBehaviour
		this.addBehaviour(tbf.wrap(new GetJobInfoBehaviour(this, newJob, binaryFile)));
	}
	
	private class GetJobInfoBehaviour extends OneShotBehaviour {
		private static final long serialVersionUID = 3514767295404355772L;
		JobNode m_job;
		byte[] m_binary;
		
		GetJobInfoBehaviour(Agent a, JobNode jn, byte[] binaryFile) {
			super(a);
			m_job = jn;
			m_binary = binaryFile;
		}
		@Override
		public void action() {
			String jobType = m_job.GetDiscreteAttribute("JobType");
			if(jobType == null) {
				System.err.println("Job Type not found");
				m_binary = null;
				return;
			}
			JobType jt = null;
			synchronized (policy) {
				for(JobType jobTypeIter : policy.GetJobTypeList()){
					if(jobTypeIter.getTypeName().compareTo(jobType)==0){
						jt = jobTypeIter;
						break;
					}
				}
			}
			if(jt == null) {
				System.err.println("No Such Job Type Exists");
				m_binary = null;
				return;
			}
			m_job.jobType = jt;
			try {
				FileOutputStream fos = new FileOutputStream(fileDirectory + m_job.UID + m_job.jobType.GetExtension());
				fos.write(m_binary);
				fos.close();				
			} catch(Exception e) {
				System.err.println("Writing binary file error");
				e.printStackTrace();
				m_binary = null;
				return;
			}
			m_job.jobType.SetJobInfo(m_job);
			m_binary = null;
			synchronized(policy) {
				policy.GetWaitingJob().add(m_job);
			}
			m_job.DisplayDetailedInfo();
		}
	}
	
	private JobNode FindAndUpdateJobNode(String line) {
		String[] subLine = line.split(" ");
		JobNode jn = null;
		for(JobNode j:policy.GetRunningJob()) {
			if(Long.parseLong(subLine[0])==j.UID) {
				jn = j;
				break;
			}
		}
		if(jn != null){
			// TODO: parse the line
		}
		
		return jn;
	}
	
	private class ListeningBehaviour extends CyclicBehaviour
	{
		private static final long serialVersionUID = 1L;

		public ListeningBehaviour(Agent agent){
			super(agent);
		}
		
		@Override
		public void action() {
			
//			System.out.println("Test Message");
			
			try {
				ACLMessage msg = myAgent.receive(MessageTemplate.MatchAll());

				if(msg == null)	{
					block();
					return;
				}
				System.out.println("Got Message");
				synchronized(policy) {
					switch(msg.getPerformative())
					{
					case ACLMessage.CONFIRM:
						/**
						 * Confirms of clusters' heart beats
						 * Message will like this:
						 *     cluster <agent's name> <agent's container name> <agent's IP> \n
						 *     load <cluster load> 
						 *     job <job type> <job's name> finished
						 *     job <job type> <job's name> running <last heartbeat time> <hasBeenExecuted> <map status> <reduce status>
						 *     job <job type> <job's name> waiting <last heartbeat time> <finished time>
						 */
						
						{
							String content = msg.getContent();
							String[] subContent = content.split("\n");
							ClusterNode cn = null;
							String aid = msg.getSender().getName();
							

							String msg1 = "\n\nMessage Sender AID: " + aid;
							
							
							System.out.println(System.currentTimeMillis());
							System.out.println(msg1);
							System.out.println("--------------------");
							System.out.println(content);
							System.out.println("--------------------");
							
							
							for(ClusterNode cnIter: policy.GetRunningCluster()) {
//								System.out.println(cnIter.clusterName + "-\n-" + cnIter.agentID + "-\n-" + aid + "-");
								if(cnIter.agentID.compareTo(aid)==0) {
//									System.out.println("AID is the same");
									cn = cnIter;
									break;
								}else {
//									System.out.println("Wrong AID " + cnIter.agentID.compareTo(aid));
								}
							}

							if(cn == null){
//								policy.MsgToRRA().add(msg);
								System.out.println("Unknown Cluster " + aid);
								return;
							}
							
							for(int line=0; line<subContent.length; line+=2) {
								if(line==0) {
									cn.load = cn.jobType.DecodeLoadInfo(subContent[1]);
								} else {
									JobNode jn = FindAndUpdateJobNode(subContent[line]);
									jn.jobType.UpdateJobNodeInfo(subContent[line+1], jn);
								}
							}											
						}
						break;
					case ACLMessage.REQUEST:
						/**
						 * Request Closing Cluster
						 * Message will like this:
						 *     Close cluster <agent's name> <agent's container name> <agent's IP>
						 */
					{
						System.out.println("Push message to RRA");
						policy.MsgToRRA().add(msg);
					}
						break;
					default:
						System.out.println("Got Message");
						System.out.println(msg.getContent());
						break;
					}
				}
				
			} catch(Exception e) {
				e.printStackTrace();
			}			
		}		
	}
}
