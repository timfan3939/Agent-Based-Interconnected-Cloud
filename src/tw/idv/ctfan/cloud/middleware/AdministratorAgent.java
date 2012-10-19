package tw.idv.ctfan.cloud.middleware;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import tw.idv.ctfan.cloud.middleware.policy.*;
import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.HadoopJobNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JavaJobNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNodeBase;
import tw.idv.ctfan.cloud.middleware.policy.data.VMMasterNode;

import jade.core.AID;
import jade.core.Agent;
import jade.core.behaviours.CyclicBehaviour;
import jade.core.behaviours.OneShotBehaviour;
import jade.core.behaviours.ThreadedBehaviourFactory;
import jade.lang.acl.ACLMessage;
import jade.lang.acl.MessageTemplate;

public class AdministratorAgent extends Agent {
	
	ThreadedBehaviourFactory tbf;	
	Policy policy;
	private static final long serialVersionUID = 1L;	
	public void setup() {
		super.setup();
		
		tbf = new ThreadedBehaviourFactory();	
		
//		policy = JobCountPolicy.GetPolicy();
//		policy = JobSizePolicy.GetPolicy();
//		policy = ExecutionTimePolicy.GetPolicy();
//		policy = JavaPolicy.GetPolicy();
		policy = JavaPolicy1.GetPolicy();
//		policy = JavaPolicy2.GetPolicy();
		
//		ClusterNode cn = new ClusterNode("null", 2048, 3, 4);
//		cn.name = "test1";
//		policy.GetCluster().add(cn);
//		JobNodeBase jn = new JavaJobNode("test", "i", "set", null);
//		jn.currentPosition = cn;
//		jn.predictTime = 50;
//		jn.hasBeenExecutedTime = 80;
//		policy.GetRunningJob().add(jn);
//		
//
//		ClusterNode cn2 = new ClusterNode("null", 2048, 3, 4);
//		cn2.name = "test1";
//		policy.GetCluster().add(cn2);
//		JobNodeBase jn2 = new JavaJobNode("test", "i", "set", null);
//		jn2.currentPosition = cn2;
//		jn2.predictTime = 50;
//		jn2.hasBeenExecutedTime = 80;
//		policy.GetRunningJob().add(jn2);
		
		this.addBehaviour(tbf.wrap(new SubmitBehaviour(this) ) );
		this.addBehaviour(tbf.wrap(new GetInfoBehaviour(this) ) );
		this.addBehaviour(tbf.wrap(new ListeningBehaviour(this) ) );
	}
		
	private class SubmitBehaviour extends CyclicBehaviour {
		private static final long serialVersionUID = 1L;
		ServerSocket server;
		
		public SubmitBehaviour(Agent agent) {
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
				
				String host = s.getInetAddress().getHostAddress();
				InputStream input = s.getInputStream();
				
				String jobInputFolder = null;
				String jobOutputFolder = null;
				String jobInputData = null;
				String jobParameter = null;
				byte[] jobBinaryFile = null;
				String jobName = null;
				String jobDeadline = null;
				
				String line = "";
				String head = "";
				String tail = "";
				
				String jobType = "";
				
				int ch;
				
				byte[] buff = new byte[0x1000];
				int bufflen = 0;
				
				while(true) {
					
					bufflen = 0;
					while( (ch = input.read()) >=0 && ch != '\n' ) {
						buff[bufflen] = (byte)ch;
						bufflen++;
					}
					
					
					if(ch!='\n') continue;
					line = new String(buff, 0, bufflen);
//					System.out.println(line);
					head = line.substring(0, line.indexOf(':'));
					tail = line.substring(line.indexOf(':')+1);
					
					if(!jobType.isEmpty()) {
						if(head.matches("Parameter")) {
							jobParameter = tail;
							bufflen = 0;
						}
						else if(head.matches("BinaryDataLength")) {
							int jobLength = Integer.parseInt(tail);
							jobBinaryFile = new byte[jobLength];
							int read = input.read(jobBinaryFile, 0, jobLength);
											
							while(read<jobLength && (ch = input.read(buff)) > 0) {
								for(int i=0; i<ch; i++) {
									jobBinaryFile[read+i] = buff[i];
								}
								read += ch;
							}
							break;
						}
						else if(jobType.matches(HadoopJobNode.JOBTYPENAME)) {
							if(head.matches("InputFolder")) {
								jobInputFolder = tail;
								bufflen = 0;
							}
							else if(head.matches("OutputFolder")) {
								jobOutputFolder = tail;
								bufflen = 0;
							}
						}
						else if(jobType.matches(JavaJobNode.JOBTYPENAME)) {
							if(head.matches("InputFile")) {
								jobInputData = tail;
								bufflen = 0;
							}
							else if(head.matches("name")) {
								jobName = tail;
								bufflen = 0;
							}
							else if(head.matches("deadline")) {
								jobDeadline = tail;
								bufflen = 0;
							}
						}						
					}
					else {
						if(head.matches("JobType")) {
							jobType = tail;
							bufflen = 0;
						}
						else {
							System.err.println("JobType Should Comes First");
						}
					}
				}				                                                       
				
				s.close();	
				
				if(!jobType.isEmpty() && jobBinaryFile!= null) {
					JobNodeBase newJob = null;
					if(jobType.matches(HadoopJobNode.JOBTYPENAME)) {
						HadoopJobNode hadoopNewJob = new HadoopJobNode(jobType, host, jobParameter, jobBinaryFile, jobInputFolder, jobOutputFolder);
						newJob = hadoopNewJob;
					}
					else if(jobType.matches(JavaJobNode.JOBTYPENAME)) {
						JavaJobNode javaNewJob = new JavaJobNode(jobType, host, jobParameter, jobBinaryFile);
						javaNewJob.inputData = jobInputData;
						javaNewJob.jobName = jobName;
						javaNewJob.deadline = Long.parseLong(jobDeadline);
						newJob = javaNewJob;
					}
					
					myAgent.addBehaviour(tbf.wrap(new GetJobInfoBehaviour(myAgent, newJob)));
				}				
				buff = null;							
			} catch( Exception e ) {
				e.printStackTrace();
			}
		}		
	}
	
	private class GetJobInfoBehaviour extends OneShotBehaviour {
		private static final long serialVersionUID = 1L;
		JobNodeBase m_job;
		
		GetJobInfoBehaviour(Agent a, JobNodeBase jn) {
			super(a);
			m_job = jn;
		}
		@Override
		public void action() {
			if(m_job.jobType.matches(HadoopJobNode.JOBTYPENAME)) {
				if(policy.GetRunningCluster().size()<=0)
					return;
				
				try {
					HadoopJobNode hJobNode = (HadoopJobNode) m_job;
					
					FileSystem fs = FileSystem.get(new URI("hdfs://10.133.200.1:9000"), new Configuration());
					
					Path path = new Path(hJobNode.inputFolder);
					
					FileStatus[] files = fs.listStatus(path);
					
					long fileSize = 0;
					int fileCount = 0;
					if(files!=null) {
						for(int i=0; i<files.length; i++) {
							if(!files[i].isDir()) {
								fileCount++;
								fileSize += files[i].getLen();
							}
						}
					}				
					hJobNode.inputFileSize = fileSize;
					hJobNode.mapNumber = fileCount;
					hJobNode.jobSize = hJobNode.binaryFile.length;	
				} catch(Exception e) {
					e.printStackTrace();
				}				
			}
			else if(m_job.jobType.matches(JavaJobNode.JOBTYPENAME)){
				JavaJobNode jJobNode = (JavaJobNode) m_job;
				jJobNode.jobSize = Long.parseLong(jJobNode.command);
			}
			policy.GetWaitingJob().add(m_job);	
			ACLMessage msg = new ACLMessage(ACLMessage.REQUEST);
			msg.addReceiver(new AID("MigrationAdmin", AID.ISLOCALNAME));
			msg.setContent("NewJobRequest");
			myAgent.send(msg);
		}
	}
	
	private class GetInfoBehaviour extends CyclicBehaviour {
		private static final long serialVersionUID = 1L;
		private final byte[] EOL = { (byte)'\r', (byte)'\n' };
		ServerSocket server;

		public GetInfoBehaviour(Agent agent) {
			super(agent);
			
			try {
				server = new ServerSocket(50071);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		public void action() {
			try {
				Socket s = server.accept();
				PrintStream output = new PrintStream(s.getOutputStream());
				BufferedInputStream input  = new BufferedInputStream(s.getInputStream());
				String request = "";
				int ch = input.read();
				
				while(ch != '\n' && ch != '\r') {
					request += (char)ch;
					ch = input.read();
				}
				
				//System.out.println(request);
				if(!request.split(" ")[0].matches("GET")) {
					s.close();
					return;
				}
				if(!request.split(" ")[1].matches("/")) {
					output.print("HTTP/1.1 501 Not Implemented" + EOL + EOL);
					output.flush();
					s.close();
					return;
				}
								
				output.print("HTTP/1.1 200 OK");
				output.write(EOL);
				output.print("Content-Type: text/html");
				output.write(EOL);
				output.write(EOL);
				
				output.flush();
				
				// HTML Starter
				output.print("<HTML>");
				
				// Header
				output.print("<HEAD><TITLE>Hybrid Cloud Information Viewer</TITLE>" +
						//"<meta http-equiv=\"refresh\" content=\"5\" />" +
						"</HEAD>");
				
				// Start of Body
				output.print("<BODY>");
				
				// cluster+running list
				output.print("<DIV>");
				output.print("<H1><img src=\"http://dmclab.csie.ntpu.edu.tw/web/media/logo_action.gif\" />Hybrid Cloud Information Viewer</H1>");
				output.print("<h3>Copyright: C.T.Fan</h3>");
				output.print("</DIV>");
				output.print("<DIV>");
				output.print("<H1>Private Cluster/Job Information</H1>");
				output.print("<TABLE style=\"text-align:center; border-collapse:collapse; border:1px black solid; width:100%\">");
				
				output.print("<THEAD>" +
								"<TR style=\"border-top:1px black solid\"><TH style=\"width:25%\">Cluster Name</TH>" +
																		 "<TH style=\"width:25%\">Remain Time</TH>" +
																		 "<TH style=\"width:25%\">Core</TH>" +
																		 "<TH style=\"width:25%\">Memory</TH></TR>"+
							 "</THEAD>");
				output.print("<TBODY>");
				
				for(ClusterNode cn : policy.GetRunningCluster()) {
					if(cn.vmMaster!=null&&cn.vmMaster.masterType!=VMMasterNode.PRIVATE)
						continue;
					long remainTime=0;
					for(JobNodeBase jn : policy.GetRunningJob()) {
						if(jn.currentPosition!=null&&jn.currentPosition.compare(cn)) {
							if(jn.predictTime-jn.hasBeenExecutedTime<0)
								remainTime=Long.MAX_VALUE;
							else if(remainTime!=Long.MAX_VALUE)
								remainTime+=(jn.predictTime-jn.hasBeenExecutedTime);
						}
					}
					output.print("<TR style=\"border-top:1px solid black\"><TD>" + cn.name + "</TD>" +
								     									  "<TD>" + remainTime + "</TD>" +
								     									  "<TD>" + cn.core+"</TD>" +
								     									  "<TD>" + cn.memory+"</TD></TR>");
					output.print("<tr><td>&nbsp;</td><td colspan=\"3\">");
					
					output.print("<table style=\"text-align:center; border-collapse:collapse; border:1px black solid;width:100%;background-color:#dddddd;\">");
					output.print("<thead>" +
							"<tr style=\"border-top:1px solid black\"><th style=\"width:20%\">Job Name</th>" +
							                                         "<th style=\"width:20%\">Job Type</th>" +
							                                         "<th style=\"width:20%\">Estimated Time</th>" +
							                                         "<th style=\"width:20%\">Running Time</th>" +
							                                         "<th style=\"width:20%\">Deadline</th></tr></thead>");
					for(JobNodeBase jn: policy.GetRunningJob()) {
						if(jn.currentPosition!=null&&jn.currentPosition.compare(cn))
							output.print("<tr style=\"border-top:1px solid black\"><td>" + jn.UID + "</td>" +
									                                              "<td>" + jn.jobName + "</td>" +
									                                              "<td>" + jn.predictTime/1000 + "</td>" +
									                                              "<td>" + jn.hasBeenExecutedTime + "</td>" +
									                                              "<td>" + jn.deadline + "</tr>");
					}
					
					output.print("</table>");
					
					output.print("</td></tr>");
				}
				output.print("</TBODY>");
				
				output.print("</TABLE>");
				
				output.print("</DIV>");
				

				output.print("<DIV>");
				output.print("<H1>Public Cluster/Job Information</H1>");
				output.print("<TABLE style=\"text-align:center; border-collapse:collapse; border:1px black solid; width:100%\">");
				
				output.print("<THEAD>" +
								"<TR style=\"border-top:1px black solid\"><TH style=\"width:25%\">Cluster Name</TH>" +
																		 "<TH style=\"width:25%\">Remain Time</TH>" +
																		 "<TH style=\"width:25%\">Core</TH>" +
																		 "<TH style=\"width:25%\">Memory</TH></TR>"+
							 "</THEAD>");
				output.print("<TBODY>");
				
				for(ClusterNode cn : policy.GetRunningCluster()) {
					if(cn.vmMaster!=null&&cn.vmMaster.masterType!=VMMasterNode.PUBLIC)
						continue;
					long remainTime=0;
					for(JobNodeBase jn : policy.GetRunningJob()) {
						if(jn.currentPosition!=null&&jn.currentPosition.compare(cn)) {
							if(jn.predictTime-jn.hasBeenExecutedTime<0)
								remainTime=Long.MAX_VALUE;
							else if(remainTime!=Long.MAX_VALUE)
								remainTime+=(jn.predictTime-jn.hasBeenExecutedTime);
						}
					}
					output.print("<TR style=\"border-top:1px solid black\"><TD>" + cn.name + "</TD>" +
								     									  "<TD>" + remainTime + "</TD>" +
								     									  "<TD>" + cn.core+"</TD>" +
								     									  "<TD>" + cn.memory+"</TD></TR>");
					output.print("<tr><td>&nbsp;</td><td colspan=\"3\">");
					
					output.print("<table style=\"text-align:center; border-collapse:collapse; border:1px black solid;width:100%;background-color:#dddddd;\">");
					output.print("<thead>" +
							"<tr style=\"border-top:1px solid black\"><th style=\"width:20%\">Job Name</th>" +
							                                         "<th style=\"width:20%\">Job Type</th>" +
							                                         "<th style=\"width:20%\">Estimated Time</th>" +
							                                         "<th style=\"width:20%\">Running Time</th>" +
							                                         "<th style=\"width:20%\">Deadline</th></tr></thead>");
					for(JobNodeBase jn: policy.GetRunningJob()) {
						if(jn.currentPosition!=null&&jn.currentPosition.compare(cn))
							output.print("<tr style=\"border-top:1px solid black\"><td>" + jn.UID + "</td>" +
									                                              "<td>" + jn.jobName + "</td>" +
									                                              "<td>" + jn.predictTime/1000 + "</td>" +
									                                              "<td>" + jn.hasBeenExecutedTime + "</td>" +
									                                              "<td>" + jn.deadline + "</tr>");
					}
					
					output.print("</table>");
					
					output.print("</td></tr>");
				}
				output.print("</TBODY>");
				
				output.print("</TABLE>");
				
				output.print("</DIV>");
				
				
				// waiting list
				
				output.print("<div>");
				output.print("<h1>Waiting Job List</h1>");
				output.print("<TABLE style=\"text-align:center; border-collapse:collapse; border:1px black solid; width:100%\">");
				
				output.print("<THEAD>" +
								"<TR style=\"border-top:1px black solid\"><TH style=\"width:34%\">Job Name</TH>" +
																	     "<TH style=\"width:33%\">Job Type</TH>" +
																	     "<TH style=\"width:33%\">Deadline</TH>" +
							 "</THEAD>");
				
				for(JobNodeBase jn:policy.GetWaitingJob()) {
					output.print("<tr style=\"border-top:1px solid black\"><td>" + jn.UID + "</td>" +
																		  "<td>" + jn.jobName + "</td>" +
																		  "<td>" + jn.deadline + "</td>" +
																		  "</tr>");
				}
				
				output.print("</TBODY>");
				
				output.print("</TABLE>");
				
				output.print("</DIV>");
				
				// finish list			
				
				output.print("<div>");
				output.print("<h1>Finished Job List</h1>");
				output.print("<TABLE style=\"text-align:center; border-collapse:collapse; border:1px black solid; width:100%\">");
				
				output.print("<THEAD>" +
								"<TR style=\"border-top:1px black solid\"><TH style=\"width:15%\">Job Name</TH>" +
																		 "<TH style=\"width:15%\">Job Type</TH>" +
																		 "<TH style=\"width:15%\">Finished Time</TH>" +
																		 "<TH style=\"width:15%\">Differences%</TH>" +
																		 "<TH style=\"width:15%\">Start Time</TH>" +
																		 "<TH style=\"width:15%\">Finish Time</TH></TR>"+
							 "</THEAD>");
				
				for(JobNodeBase jn:policy.GetFinishJob()) {
					output.print("<tr style=\"border-top:1px solid black\"><td>" + jn.UID + "</td>" +
																		  "<td>" + jn.jobName + "</td>" +
																		  "<td>" + jn.executeTime + "</td>" +
																		  "<td>" + (((double)jn.predictTime - (double)jn.executeTime)/(double)jn.executeTime) + "</td>" +
																		  "<td>" + jn.startTime+ "</td>" +
																		  "<td>" + jn.finishTime + "</td></tr>");
				}
				
				output.print("</TBODY>");
				
				output.print("</TABLE>");
				
				output.print("</DIV>");
				
				// End of Body
				output.print("</BODY>");
				
				// end of HTML
				output.print("</HTML>");
				
				output.flush();
				
				s.close();
				
				
			} catch(Exception e)
			{
				e.printStackTrace();
			}
			
		}
		
	}
	
	private class ListeningBehaviour extends CyclicBehaviour
	{
		private static final long serialVersionUID = 1L;

		public ListeningBehaviour(Agent agent){
			super(agent);
		}

		@Override
		public void action() {
			
			//System.out.println("Test Message");
			
			try {
				ACLMessage msg = myAgent.receive(MessageTemplate.MatchAll());
				
				if(msg == null)	{
					block();
					return;
				}
				
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
						String[] line = subContent[0].split(" ");
						ClusterNode cn = null;
						JobNodeBase     jn = null;
						boolean  found = false;
						
						//System.out.println(content);
						
						if(line[0].matches("cluster")){
							found = false;
							for(int i=0; i<policy.GetRunningCluster().size()&&!found; i++)	{
								found = policy.GetRunningCluster().get(i).compare(line[1], line[2], line[3]);
								if(found)
									cn = policy.GetRunningCluster().get(i);
							}
							
							if(!found){
								cn = new ClusterNode(line[1], line[2], line[3]);
								policy.GetRunningCluster().add(cn);
							} else	{
								line = subContent[1].split(" ");
								
								for(int i=0; i<line.length; i++){
									if(line[i].matches("load"))	{
										i++;
										cn.load = Integer.parseInt(line[i]);
									} else if(line[i].matches("maxMap")) {
										i++;
										cn.maxMapSlot = Integer.parseInt(line[i]);
									} else if(line[i].matches("maxReduce")) {
										i++;
										cn.maxReduceSlot = Integer.parseInt(line[i]);
									}
								}
							}
							
							if(subContent.length > 2){
								for(int i=2; i<subContent.length; i++){
									found = false;
									line = subContent[i].split(" ");
									
									if(line[0].matches("job")){
										for(int j=0; j<policy.GetRunningJob().size(); j++){
											if(Long.parseLong(line[2].substring(3))==policy.GetRunningJob().get(j).UID)	{
												jn = policy.GetRunningJob().get(j);
												found = true;
											}
										}
										
										if(found){
											jn.currentPosition = cn;
											if(line[3].matches("running"))	{
												jn.lastExist = Long.parseLong(line[4]);
												jn.jobStatus = HadoopJobNode.RUNNING;
												jn.hasBeenExecutedTime = Long.parseLong(line[5]);
												if(line[1].matches(HadoopJobNode.JOBTYPENAME)) {
													((HadoopJobNode)jn).mapStatus = Integer.parseInt(line[6]);
													((HadoopJobNode)jn).reduceStatus = Integer.parseInt(line[7]);
												}
												else if(line[1].matches(JavaJobNode.JOBTYPENAME)){
													
												}
											}
											else if(line[3].matches("waiting")) {
												jn.lastExist = Long.parseLong(line[4]);
												jn.jobStatus = HadoopJobNode.WAITING;
											}
											else if(line[3].matches("finished")) {
												System.out.println(myAgent.getLocalName() + ": got finish job");
												policy.GetRunningJob().remove(jn);
												policy.GetFinishJob().add(jn);
												jn.finishTime = System.currentTimeMillis();
												
												jn.executeTime = Long.parseLong(line[4]);
												
												System.out.println("Difference: " + ((double)jn.predictTime - (double)jn.executeTime)/(double)jn.executeTime);
												System.out.println("Setting: " + jn.command + " time: " + jn.executeTime);
												jn.jobStatus = HadoopJobNode.FINISHED;
												jn.currentPosition = new ClusterNode(jn.currentPosition.maxMapSlot, jn.currentPosition.maxReduceSlot);
												
												/// TODO:add finished output file size procedure 
											}
										} else {
											System.err.println("Error");
											System.err.print(subContent[i]);
										}										
									}									
								}
							}
						}						
					}
					break;
				case ACLMessage.REQUEST:
					break;
				default:
					System.out.println("Got Message");
					System.out.println(msg.getContent());
					break;
				}
				
			} catch(Exception e) {
				e.printStackTrace();
			}			
		}		
	}
}
