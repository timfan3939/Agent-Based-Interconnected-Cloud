package tw.idv.ctfan.cloud.middleware;

import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import tw.idv.ctfan.cloud.middleware.policy.*;
import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNode;
import jade.core.AID;
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
	
	public static final String NAME = "SyMA";
	
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
						try {
							long value = Long.parseLong(tail);
							jn.AddContinuousAttribute(head, value);
						} catch (NumberFormatException e) {
							jn.AddDiscreteAttribute(head, tail);
						}
					}
				}
				
				s.close();
				
				if(jobBinaryFile!=null) {
					myAgent.addBehaviour(tbf.wrap(new GetJobInfoBehaviour(myAgent, jn)));
				}				
				
				buff = null;							
			} catch( Exception e ) {
				e.printStackTrace();
			}
		}		
	}
	
	public void SubmitJob(JobNode newJob) {
		this.addBehaviour(tbf.wrap(new GetJobInfoBehaviour(this, newJob)));
	}
	
	private class GetJobInfoBehaviour extends OneShotBehaviour {
		private static final long serialVersionUID = 1L;
		JobNode m_job;
		
		GetJobInfoBehaviour(Agent a, JobNode jn) {
			super(a);
			m_job = jn;
		}
		@Override
		public void action() {
			
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
						JobNode     jn = null;
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
								policy.OnNewClusterArrives(cn);
								//policy.GetRunningCluster().add(cn);
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
					/**
					 * Request Closing Cluster
					 * Message will like this:
					 *     Close cluster <agent's name> <agent's container name> <agent's IP>
					 */
				{
					String content = msg.getContent();
					System.out.println(content);
					String[] subContent = content.split(" ");
					if(subContent.length==5)
					if(subContent[0].matches("Close"))
					if(subContent[1].matches("cluster")) {
						policy.OnOldClusterLeaves(new ClusterNode(subContent[2], subContent[3], subContent[4]));
					}
				}
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
