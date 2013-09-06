package tw.idv.ctfan.cloud.middleware.MapReduce;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;

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
	private String m_hadoopHome;
	
	private static int maxExecuteJobNumber = 2;
	
	private String m_masterIP;
	
	private JobClient m_mapredClient;
	private FileSystem m_hdfs;
	
	private ArrayList<JobListNode>     m_jobList     = new ArrayList<JobListNode>();
	
	


	public void setup()
	{
		super.setup();
		
		Object[] args = this.getArguments();
		if(args.length != 3)
		{
			System.out.println("Usage: HadoopHome JarHome MasterIP");
		}
		m_hadoopHome = (String)args[0];
		m_jarPath    = (String)args[1];
		m_masterIP   = (String)args[2];
		
		try {
			m_mapredClient = new JobClient(new InetSocketAddress("localhost", 9001),new Configuration());
		} catch (Exception e) {
			System.err.println("Error While Connecting to JobTracker");
			e.printStackTrace();
			System.err.println("The Agent will be terminated");
			this.doDelete();
			return;
		}
		
		try {
			m_hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration());
		} catch (Exception e) {
			System.err.println("Error While Connecting to Filesystem");
			e.printStackTrace();
			System.err.println("The Agent will be terminated");
			this.doDelete();
			return;
		}
	
		System.out.println("Hadoop Home: " + m_hadoopHome + ":");
		System.out.println("Jar    Path: " + m_jarPath    + ":");
		System.out.println("Master IP  : " + m_masterIP   + ":");
		
		
		
		this.addBehaviour(new MessageListeningBehaviour(this));
		this.addBehaviour(new HeartBeatBehaviour(this, 3000));
	}
	
	private class MessageListeningBehaviour extends CyclicBehaviour
	/*
	 * Request	: new job
	 * Propose	: job Execution (but have to wait)
	 * Inform	: job execution/migration
	 * Confirm	: job exists 
	 */
	{
		private static final long serialVersionUID = 1L;
		
		public MessageListeningBehaviour(Agent agent)
		{
			super(agent);
		}
		
		
		@Override
		public void action() {			
			try {
				
				MessageTemplate mt = MessageTemplate.MatchAll();
				ACLMessage msg = myAgent.receive(mt);				
				JobListNode JNode;
				if(msg == null)
				{
					block();
					return;
				}
				//System.out.println(myAgent.getName() + ": Got Message");
				
				switch(msg.getPerformative())
				{
					case ACLMessage.REQUEST:
					{						
						byte[] program = msg.getByteSequenceContent();
						
						myAgent.addBehaviour(new NewJobBehaviour(myAgent, program ));
						//System.out.println("New Job Request");
					}
						break;
					case ACLMessage.INFORM:
					{
						String info = msg.getContent();
						if(info.matches("Migration"))
						{
							for(int i=0; i<m_jobList.size(); i++)
							{
								JNode = m_jobList.get(i);
								if(JNode.name.compareTo(msg.getSender().getLocalName())==0)
								{
									myAgent.addBehaviour(new TerminateJobBehaviour(myAgent, JNode));
									m_jobList.remove(i);
									break;
								}
							}
						}
						else
						{
							String agentName = msg.getSender().getName();
							JobListNode jn = null;
							
							for(int i=0; i<m_jobList.size(); i++)
							{
								if(agentName.compareTo(m_jobList.get(i).name) ==0)
								{
									jn = m_jobList.get(i);
									break;
								}
							}
							
							if(jn!=null)
							{
								jn.jobID = info;
							}
						}
					}
						break;
					case ACLMessage.PROPOSE:
					case ACLMessage.CONFIRM:
					{
						JNode = null;
						for(int i=0; i<m_jobList.size(); i++){
							JNode = m_jobList.get(i);
							if(JNode.name.compareTo(msg.getSender().getLocalName()) ==0)	{
								JNode.SetExist();
								if(JNode.jobID==null || JNode.jobID.isEmpty())
									JNode.jobID = msg.getContent();
								break;
							}
						}
						if(JNode==null)
						{
							System.err.println(myAgent.getName() + " No Job Found!");
						}
					}
						break;
					default:
						System.out.println(myAgent.getName() + " Got unknow messate " + msg.getContent());
						break;
				}
				
				
			} catch(Exception e)
			{
				e.printStackTrace();
			}
			
			
		}
		
	}
	
	private class NewJobBehaviour extends Behaviour
	{
		private static final long serialVersionUID = 1L;
		byte[] m_data;
		boolean hasParced;
		
		// Job's properties
		String 		jobUID;
		String		jobInputFolder;
		String 		jobOutputFolder;
		int			jobLength;
		String		jobParameter;
		byte[]		jobBinaryFile;
		boolean 	doneYet = false;
				
		public NewJobBehaviour(Agent agent, byte[] program)
		{
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
			
			if(!hasParced)
			{
				ByteArrayInputStream dataInput = new ByteArrayInputStream(m_data);
				
				
				
				while(dataInput.available()>0)
				{
					buffLen = 0;
					while( (c = dataInput.read()) != '\n' )
					{
						buff[buffLen] = (byte)c;
						buffLen++;
					}
					
					String line = new String(buff, 0, buffLen);
					String head = line.substring(0,	line.indexOf(":"));
					String tail = line.substring(line.indexOf(":")+1);
					
					if(head.matches("UID"))	{
						jobUID = tail;
						infoToJobAgent += ("UID:" + jobUID + "\n");
					}
					else if(head.matches("InputFolder")){
						jobInputFolder = tail;
						infoToJobAgent +=  ("InputFolder:" + jobInputFolder + "\n");
					}
					else if(head.matches("OutputFolder")){
						jobOutputFolder = tail;
						infoToJobAgent += ("OutputFolder:" + jobOutputFolder + "\n");
					}
					else if(head.matches("BinaryDataLength")){
						try {
							jobLength = Integer.parseInt(tail);
							jobBinaryFile = new byte[jobLength];
							dataInput.read(jobBinaryFile, 0, jobLength);
						} catch(Exception e)
						{
							System.err.println("Original Line:" + line);
							e.printStackTrace();
							return;
						}
					}				
					else if(head.matches("Parameter")){
						jobParameter = tail;
					}
				}			
				
				hasParced = true;
			
			}
			try {
				
				File f = new File(m_jarPath + "/" + "job" + jobUID + ".jar");
				
				if(!f.exists()) {
					FileOutputStream output = new FileOutputStream(m_jarPath + "/" + "job" + jobUID + ".jar", false);
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
				jn.inputFolder = jobInputFolder;
				jn.outputFolder = jobOutputFolder;
				m_jobList.add(jn);
								
				myAgent.getContainerController().createNewAgent("job" + jobUID,
																tw.idv.ctfan.cloud.middleware.MapReduce.JobAgent.class.getName(),
																parameterList.toArray()).start();
				System.out.println("=====Agent Start=====");
				doneYet = true;
			}
			catch(StaleProxyException e) {
				System.out.println("===Agent Exception===");
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
	
	private class TerminateJobBehaviour extends OneShotBehaviour
	{
		private static final long serialVersionUID = 1L;		
		JobListNode jn;
		
		public TerminateJobBehaviour(Agent a, JobListNode agent) {
			super(a);
			this.jn = agent;
			System.out.println(a.getName() + " Migrate " + jn.name);
		}

		@Override
		public void action() {			
			RunningJob runningJob = null;
						
			if(jn != null) {
				if(jn.jobID == null) {
					return;
				}
				else {
					try {
						JobStatus[] jobStatus = m_mapredClient.getAllJobs();
						for(int i=0; i<jobStatus.length; i++) {
							//System.out.println(jobStatus[i].getJobID().toString() + "\t" + jn.jobID);
							if(jobStatus[i].getJobID().toString().compareTo(jn.jobID)==0) {
								runningJob = m_mapredClient.getJob(jobStatus[i].getJobID());
								break;
							}
						}						
						if(runningJob != null) {
							runningJob.killJob();
							
							Path path = new Path(jn.outputFolder);
							if(m_hdfs.exists(path)) {
								m_hdfs.delete(path, true);
							}						
						}						
					} catch(Exception e) {
						e.printStackTrace();
					}
				}
			}
			else
				System.out.println("Error, Can't find migrated job");
		}		
	}
	
	private class JobListNode
	{
		public String 	name;
		public String 	inputFolder;
		public String 	outputFolder;
		
		public String 	jobID;
		
		public long   	lastExist;
		
		public int		hasBeenExecute;		
		
		public JobListNode(String name)
		{
			this.name = name;
			
			inputFolder = null;
			outputFolder = null;
			jobID = null;
			lastExist = -1;
			hasBeenExecute = -1;
		}
		
		public void SetExist()
		{
			lastExist = System.currentTimeMillis();
		}
	}
	
	
	/**
	 * Confirms of clusters' heart beats
	 * Message will like this:
	 *     cluster <agent's name> <agent's container name> <agent's IP> \n
	 *     load <cluster load> 
	 *     job <job's name> finished
	 *     job <job's name> running <map status> <reduce status> <last heartbeat time>
	 *     job <job's name> waiting <last heartbeat time>
	 */
	private class HeartBeatBehaviour extends TickerBehaviour
	{
		private static final long serialVersionUID = 1L;
		
		private int lastAskExecuteJob;

		public HeartBeatBehaviour(Agent a, long period) {
			super(a, period);
			
			lastAskExecuteJob = 0;
		}

		@Override
		protected void onTick() {
			ACLMessage heartBeat = new ACLMessage(ACLMessage.CONFIRM);
			
			AID reciever = new AID("CloudAdmin@" + m_masterIP + ":1099/JADE", AID.ISGUID);
			reciever.addAddresses("http://" + m_masterIP + ":7778/acc");
			heartBeat.addReceiver(reciever);
			
			String content = "cluster ";
			
			// send who am I
			content += myAgent.getLocalName() + " " 
			         + myAgent.here().getName() + " " 
			         + myAgent.getHap().split(":")[0] + "\n";
			
			//send System load
			ClusterStatus status;
			try {
				status = m_mapredClient.getClusterStatus();
				int load = (int)((status.getMaxMapTasks() - status.getMapTasks())/(status.getMaxMapTasks()==0?status.getMaxMapTasks():99999));
				content += ("load " + load);
				content += (" maxMap " + m_mapredClient.getDefaultMaps());
				content += (" maxReduce " + m_mapredClient.getDefaultReduces());
				content += "\n";
			} catch (Exception e) {
				e.printStackTrace();
				content += ("load " + -1 + "\n");
			}	
			
			//send Job Information
			int runningSpaceLeft = maxExecuteJobNumber;
			try {
				long currentTime = System.currentTimeMillis();
				JobStatus[] jobStatus = m_mapredClient.getAllJobs();
				for(int i=0; i<m_jobList.size(); i++)
				{
					JobListNode JBNode = m_jobList.get(i);
					
					if(JBNode.jobID == null || JBNode.jobID.isEmpty())
					{
						content += ("job " + JBNode.name + " waiting " + (currentTime-JBNode.lastExist)) + "\n";
					}
					else
					{						
						if(jobStatus!=null) {
							for(int jobCount=0; jobCount<jobStatus.length; jobCount++) {
								if(jobStatus[jobCount].getJobID().toString().compareTo(JBNode.jobID) ==0)
								{
									if(jobStatus[jobCount].getRunState()==JobStatus.SUCCEEDED)
									{
										TaskReport[] tReport = m_mapredClient.getMapTaskReports(jobStatus[jobCount].getJobID());
										TaskReport[] rReport = m_mapredClient.getReduceTaskReports(jobStatus[jobCount].getJobID());
										
										long start=0;
										long end=0;
										for(int t=0; t<tReport.length; t++)
											if(t==0)
												start = tReport[t].getStartTime();
											else
												if(start>tReport[t].getStartTime())
													start = tReport[t].getStartTime();
										for(int r=0; r<rReport.length; r++)
											if(r==0)
												end = rReport[r].getFinishTime();
											else
												if(end<rReport[r].getFinishTime())
													end = rReport[r].getFinishTime();										
										
										content += ("job " + JBNode.name + " finished " + (end - start) + "\n");
										m_jobList.remove(i);
										i--;
									}
									else if(jobStatus[jobCount].getRunState()==JobStatus.RUNNING)
									{
										runningSpaceLeft--;
										content += ("job " + JBNode.name + " running " + 
												   (int)(jobStatus[jobCount].mapProgress()*100) + " " + 
												   (int)(jobStatus[jobCount].reduceProgress()*100) + " " +
												   (currentTime-JBNode.lastExist) + "\n");
									}
								}
							}
						}						
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}						
			//System.out.println(myAgent.getName() + " Heart Beats :" + content);
			heartBeat.setContent(content);
			myAgent.send(heartBeat);
			
			
			if( (runningSpaceLeft > 0 && m_jobList.size()>0 && lastAskExecuteJob<=0) ||
				(runningSpaceLeft == maxExecuteJobNumber && m_jobList.size()>0)        )
			{
				//System.out.println("Send New Job");
				
				JobListNode jn = null;
				
				for(int i=0; i<m_jobList.size(); i++)
				{
					JobListNode curr = m_jobList.get(i);
					
					if(curr.hasBeenExecute>0)
					{
						curr.hasBeenExecute--;
						curr = null;
					}
					else if(curr.hasBeenExecute==0 && curr.jobID == null)
					{
						curr.hasBeenExecute = -1;
					}
					
					if(curr!= null && curr.hasBeenExecute == -1)
					{
						if(jn!=null)
						{
							if(jn.name.compareTo(curr.name) >0)
								jn = curr;
						}
						else
							jn = curr;						
					}
				}
				
				
				if(jn != null)
				{
					String jobName = jn.name;
					jn.hasBeenExecute = 30;				
					
					ACLMessage msg = new ACLMessage(ACLMessage.CONFIRM);
					AID aid = new AID(jobName + "@" + myAgent.getHap(), AID.ISGUID);
					//System.out.println("Send to " + aid.getName());
					msg.addReceiver(aid);
					msg.setContent(m_hadoopHome);
					
					myAgent.send(msg);
					lastAskExecuteJob = 10;
				}
			}
			else
				lastAskExecuteJob--;
		}		
	}
}

