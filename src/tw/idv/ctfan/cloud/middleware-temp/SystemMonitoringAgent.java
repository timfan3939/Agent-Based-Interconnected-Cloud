package tw.idv.ctfan.cloud.Middleware;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.TimeZone;

import tw.idv.ctfan.cloud.Middleware.Cluster.JobType;
import tw.idv.ctfan.cloud.Middleware.policy.*;
import tw.idv.ctfan.cloud.Middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.Middleware.policy.data.JobNode;
import jade.core.Agent;
import jade.core.behaviours.Behaviour;
import jade.core.behaviours.CyclicBehaviour;
import jade.core.behaviours.OneShotBehaviour;
import jade.core.behaviours.ThreadedBehaviourFactory;
import jade.core.behaviours.TickerBehaviour;
import jade.lang.acl.ACLMessage;
import jade.lang.acl.MessageTemplate;
import jade.wrapper.StaleProxyException;

public class SystemMonitoringAgent extends Agent {
	
	private static final long serialVersionUID = -5271213701466983534L;
	ThreadedBehaviourFactory tbf;	
	Policy policy;	
	
	/**
	 * Where the binary files will be stored.
	 */
	private final String fileDirectory = "C:\\ctfan\\MYPAPER\\testfile\\middlewareFile\\";
	/**
	 * I'm busy on other feature.  This feature
	 */
//	public static final String NAME = "SyMA@120.126.145.102:1099/JADE";
	public static final String NAME = "SyMA@10.133.200.245:1099/JADE";
	
	public void setup() {
		super.setup();
		
		tbf = new ThreadedBehaviourFactory();	
		
		policy = MultiTypePolicy.GetPolicy();
				
		this.addBehaviour(tbf.wrap(new SubmitBehaviour(this) ) );
		this.addBehaviour(tbf.wrap(new HTTPServerBehaviour(this, policy) ) );
		this.addBehaviour(tbf.wrap(new ListeningBehaviour(this) ) );
	}
	
	/**
	 * Quick way to add a threaded behaviour from other behaviour.
	 * @param b
	 */
	public void AddTbfBehaviour(Behaviour b) {
		this.addBehaviour(this.tbf.wrap(b));
	}
	
	/**
	 * Quick submit behaviour.  Port 50031 is used.
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
				File saveFile=new File("C:\\ctfan\\MYPAPER\\testfile\\exetime\\"+jn.UID+"exetime.txt");
				try
				{
					//==格式化
					SimpleDateFormat nowdate = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
					//==GMT標準時間往後加八小時
					nowdate.setTimeZone(TimeZone.getTimeZone("GMT+8"));
					//==取得目前時間
					String sdate = nowdate.format(new java.util.Date());
					FileWriter fwriter=new FileWriter(saveFile, true);
					fwriter.write(jn.UID + " starttime:" + sdate+ ",time:"+System.currentTimeMillis()+"\n");
					fwriter.close();
				}
				catch(Exception e)
				{
					e.printStackTrace();
				}
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
					
					//parse job
					if(ch!='\n') continue;
					line = new String(buff, 0, bufflen);
//					System.out.println(line);
					head = line.substring(0, line.indexOf(':'));
					tail = line.substring(line.indexOf(':')+1);
					
					//System.out.println("line:"+line+",head:"+head+",tail:"+tail);
					
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
						//System.out.println("line:::"+line+",head:::"+head+",tail:::"+tail);
						jn.AddDiscreteAttribute(head, tail);
					}
				}
				s.close();
					
				if(jobBinaryFile!=null) {
					
					jn.DisplayDetailedInfo();
					//System.out.println("myAgentLocalName:"+myAgent.getLocalName()+",job UID:" + jn.UID +",size"+ jobBinaryFile.length);
					myAgent.addBehaviour(tbf.wrap(new GetJobInfoBehaviour(myAgent, jn, jobBinaryFile)));
				}				
				
				buff = null;							
			} catch( Exception e ) {
				e.printStackTrace();
			}
		}		
	}
	
	/**
	 * Used by the HTTP behaviour.
	 * @param newJob
	 * @param binaryFile
	 */
	public void SubmitJob(JobNode newJob, byte[] binaryFile) {
		// Method for HTTPServerBehaviour
		this.addBehaviour(tbf.wrap(new GetJobInfoBehaviour(this, newJob, binaryFile)));
	}
	
	/**
	 * Job's info is handled here.  For example, providing the size of a job.
	 * @author C.T.Fan
	 *
	 */
	private class GetJobInfoBehaviour extends OneShotBehaviour {
		private static final long serialVersionUID = 3514767295404355772L;
		JobNode m_job;
		byte[] m_binary;
		
		GetJobInfoBehaviour(Agent a, JobNode jn, byte[] binaryFile) {
			super(a);
			m_job = jn;
			m_job.submitTime = System.currentTimeMillis();
			m_binary = binaryFile;
		}
		@Override
		public void action() {
			String jobType = m_job.GetDiscreteAttribute("JobType");
			System.out.println("\njobType:"+jobType);
			if(jobType == null) {
				System.err.println("Job Type not found");
				m_binary = null;
				return;
			}
			JobType jt = null;
			synchronized (policy) {
				for(JobType jobTypeIter : policy.GetJobTypeList()){
//					System.out.println("\nforsize"+policy.GetJobTypeList());
					if(jobTypeIter.getTypeName().compareTo(jobType)==0){
						jt = jobTypeIter;
						//System.out.println("\njt"+jt);
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
//			System.out.println("test1:"+m_job.jobType.getTypeName());
			if(m_job.jobType.getTypeName() != "Workflow"){
				System.out.println("not workflow job");
				synchronized(policy) {
					policy.AppendNewJob(m_job);
				}
			}
			else{
				
				System.out.println("workflow job");
				ArrayList<String> cmd = new ArrayList<String>();
				cmd.add(myAgent.getLocalName());
				cmd.add(fileDirectory);
				cmd.add("job" + m_job.UID + jt.GetExtension());
				//cmd.add(OnEncodeNewJobAgent(m_job));
				try {
					myAgent.getContainerController().createNewAgent(Long.toString(m_job.UID), tw.idv.ctfan.cloud.Middleware.Workflow.WorkflowAgent.class.getName(), cmd.toArray()).start();
				} catch (StaleProxyException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
//			m_job.DisplayDetailedInfo();
		}
	}
	
	/**
	 * Simple function used to update informations about a job.<br/>
	 * This function only deal with internal data.<br/>
	 * @param line
	 * @return
	 */
	private JobNode FindAndUpdateJobNode(String line) {
		String[] subLine = line.split(" ");
		JobNode jn = null;
		for(JobNode j:policy.GetRunningJob()) {
			if(Long.parseLong(subLine[0])==j.UID) {
				jn = j;
				if(subLine[1].matches("Finished")) {
					policy.GetRunningJob().remove(jn);
					policy.GetFinishJob().add(jn);
					
/************************************/
					
					if(jn.UID == Long.valueOf(jn.getdispatchsequence(Integer.valueOf(String.valueOf((jn.getdispatchnum()-1)))))){
						
						File saveFile=new File("C:\\ctfan\\MYPAPER\\testfile\\exetime\\"+jn.UID/1000+"exetime.txt");
						try
						{
							//==格式化
							SimpleDateFormat nowdate = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
							//==GMT標準時間往後加八小時
							nowdate.setTimeZone(TimeZone.getTimeZone("GMT+8"));
							//==取得目前時間
							String sdate = nowdate.format(new java.util.Date());
							FileWriter fwriter=new FileWriter(saveFile, true);
							fwriter.write(jn.UID/1000 + " endtime:" + sdate
									+ ",time:"+System.currentTimeMillis()+"\n");
							fwriter.close();
						}
						catch(Exception e)
						{
							e.printStackTrace();
						}
					}
					
/**************************************/
					jn.finishTime = System.currentTimeMillis();
					jn.completionTime = Long.parseLong(subLine[3]);
					String name = jn.GetDiscreteAttribute("Name");
					String cmd = jn.GetDiscreteAttribute("Command");
					((MultiTypePolicy)policy).WriteLog("<tr style=\"border-top:1px solid black\"><td>" + jn.UID + "</td>" +
							  "<td>" + jn.jobType.getTypeName() + "</td>" +
							  "<td>" + (name==null?"N/A":name) + "</td>" +
							  "<td>" + (cmd==null?"N/A":cmd) + "</td>" +
							  "<td>" + jn.runningCluster.clusterName + "</td>" +
							  "<td>" + jn.submitTime + "</td>" +
							  "<td>" + jn.startTime + "</td>" +
							  "<td>" + jn.finishTime + "</td>" +
							  "<td>" + jn.completionTime + "</td>" +
							  "<td>" + jn.GetContinuousAttribute("PredictionTime") + "</td>" +
							  "<td>" + jn.deadline + "</td>" +
							  "</tr>");
				}
				else if(subLine[1].equals("Running")){
					jn.lastSeen = Long.parseLong(subLine[2]);
					jn.completionTime = Long.parseLong(subLine[3]);
				}
				else if(subLine[1].matches("Waiting")) {
					jn.lastSeen = Long.parseLong(subLine[2]);
				}
				break;
			}
		}
		if(jn != null){
			// TODO: parse the line
		}
		
		return jn;
	}
	
	/**
	 * {@link ACLMessage} listening method.
	 * @author C.T.Fan
	 *
	 */
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
//				System.out.println("Got Message");
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
							

//							String msg1 = "\n\nMessage Sender AID: " + aid;
//							
//							
//							System.out.println(System.currentTimeMillis());
//							System.out.println(msg1);
//							System.out.println("--------------------");
//							System.out.println(content);
//							System.out.println("--------------------");
							
							
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
									cn.load = cn.jobType.DecodeClusterLoadInfo(subContent[1]);
								} else {
									JobNode jn = FindAndUpdateJobNode(subContent[line]);
									jn.jobType.UpdateJobNodeInfo(subContent[line+1], jn);
								}
							}				
//							System.out.println("Waiting Jobs: " + policy.GetWaitingJob().size());
//							System.out.println("Running Jobs: " + policy.GetRunningJob().size());
//							System.out.println("Finished Jobs: " + policy.GetFinishJob().size());
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
