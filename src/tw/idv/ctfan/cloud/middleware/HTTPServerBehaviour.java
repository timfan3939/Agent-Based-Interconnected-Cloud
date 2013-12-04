package tw.idv.ctfan.cloud.middleware;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.HashMap;
  
import tw.idv.ctfan.cloud.middleware.Cluster.JobType;
import tw.idv.ctfan.cloud.middleware.policy.Policy;
import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNode;
import tw.idv.ctfan.cloud.middleware.policy.data.VMController;

import jade.core.Agent;
import jade.core.behaviours.CyclicBehaviour;
import jade.core.behaviours.OneShotBehaviour;


public class HTTPServerBehaviour extends CyclicBehaviour {
	private static final long serialVersionUID = 1L;
	private final byte[] EOL = { (byte)'\r', (byte)'\n' };
	ServerSocket server;
	Policy policy;
	SystemMonitoringAgent myAgent;
	
	static final Date m_initTime = new Date();
	
	private static final int HTTP_POST = 0x301;
	private static final int HTTP_GET  = 0x302;

	public HTTPServerBehaviour(SystemMonitoringAgent agent, Policy policy) {
		super(agent);
		myAgent = agent;
		this.policy = policy;
		
		try {
			server = new ServerSocket(50071);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * The states used while parcing the HTTP content
	 * @author C.T.Fan
	 *
	 */
	private enum PARCING_STATE {
		stateBoundary, stateDescriptor, stateReadData,
		handleJobType,handleAttribute, handleValue, handleBinaryFile
	}
	
	@Override
	public void action() {
		try {
			Socket client = server.accept();
			
			boolean endOfHeader = false;		
			
			BufferedInputStream input = new BufferedInputStream(client.getInputStream());
			
			int connectionType = 0;
			String file = "";
			String boundary = "";
			int line = 0;
			
			do {
				boolean endOfField = false;
				String field = "";
				int ch = input.read();
				do {
					if(ch=='\r') {
						if((ch=input.read())=='\n'){
							endOfField = true;
						} else {
							field += '\r';
						}
					} else {
						field += (char)ch;
						ch = input.read();
					}
				} while (!endOfField);
				if(field.isEmpty()) {
					endOfHeader = true;
					break;
				}
				System.out.println("" + (++line) + ": " + field);
				
				String[] info = field.split(" ");
				if(info.length==0) break;
				
				if(line == 1) {
					if(info[0].matches("GET")) {
						connectionType = HTTP_GET;
						file = info[1];					
					} else if(info[0].matches("POST")) {
						connectionType = HTTP_POST;
						file = info[1];
					} else {
						myAgent.AddTbfBehaviour(new Error501Response(myAgent, client));
						return;
					}
				} else {
					if(info[0].matches("Content-Type:")) {
						if(info[1].matches("multipart/form-data;")) {
							String bound[] = info[2].split("=");
							if(bound.length == 2 && bound[0].matches("boundary"))
								boundary = bound[1];
							else {
								myAgent.AddTbfBehaviour(new Error501Response(myAgent, client));
								return;
							}
						}
						else {
							myAgent.AddTbfBehaviour(new Error501Response(myAgent, client));
							return;
						}							
					}
				}				
			} while(!endOfHeader);
			
//			System.out.println("Reading header done");
			
			// Read body content
			ByteArrayOutputStream bodyContentStream;
//			int size = 0;
			
			if(connectionType==HTTP_POST) {
				boolean endOfMessage = false;				
				
				// Parsing POST multi-form state
				PARCING_STATE state = PARCING_STATE.stateBoundary;
				
				// Parsing multiple message state
				PARCING_STATE handling = null;
				
				// Data received
				String jobType = "";
				byte[] binaryFile = null;
				JobNode newJob = new JobNode();
				HashMap<Long, String> Attribute = new HashMap<Long, String>();
				HashMap<Long, String> Value     = new HashMap<Long, String>();
				long number = 0;
				
				do {
					switch (state) {
					case stateBoundary: {
						bodyContentStream = new ByteArrayOutputStream();
						boolean endOfLine = false;
						while (!endOfLine) {
							int ch = input.read();
							if(ch == '\r')
								if( (ch=input.read()) =='\n' ){
									endOfLine = true;
								} else {
									bodyContentStream.write('r');
									bodyContentStream.write(ch);
								}
							else
								bodyContentStream.write(ch);
						}
						String b = new String(bodyContentStream.toByteArray());
						if(b.compareTo("--"+boundary)==0) {
//							System.out.println("--"+boundary);
							state = PARCING_STATE.stateDescriptor;
						} else if(b.compareTo("--"+boundary+"--")==0){
//							System.out.println("--"+boundary+"--");
							endOfMessage = true;
						} else if(b.compareTo(boundary)==0){
//							System.out.println(boundary);
							state = PARCING_STATE.stateDescriptor;
						} else if(b.compareTo(boundary+"--")==0){
//							System.out.println(boundary+"--");
							endOfMessage = true;
						}
						bodyContentStream = null;
					}	break;
					case stateDescriptor:{
						bodyContentStream = new ByteArrayOutputStream();
						boolean endOfLine = false;
						while (!endOfLine) {
							int ch = input.read();
							if(ch == '\r')
								if( (ch=input.read()) =='\n' ){
									endOfLine = true;
								} else {
									bodyContentStream.write('r');
									bodyContentStream.write(ch);
								}
							else
								bodyContentStream.write(ch);
						}
						
						if(bodyContentStream.size()==0)
							state = PARCING_STATE.stateReadData;
//						else
//							System.out.println(new String(bodyContentStream.toByteArray()));
						String info = new String(bodyContentStream.toByteArray());
						String subInfo[] = info.split(" ");
						if(subInfo[0].matches("Content-Disposition:")&&subInfo[1].matches("form-data;")) {
							String tag = subInfo[2].split("=")[1].split("\"")[1];
//							System.out.println("tag: " + tag);
							if(tag.matches("jobType")) {
								handling = PARCING_STATE.handleJobType;
							} else if(tag.matches("binaryFile")) {
								handling = PARCING_STATE.handleBinaryFile;
//								jobName = subInfo[3].split("=")[1].split("\"")[1];
//								System.out.println("jobName: " + jobName);
							} else if(tag.matches("attribute\\d")) {
								handling = PARCING_STATE.handleAttribute;
								try {
									number = Long.parseLong(tag.substring("attribute".length()));
								} catch (NumberFormatException e) {
									System.err.println(tag);
									e.printStackTrace();
									number = -1;
								}
							} else if(tag.matches("value\\d")) {
								handling =PARCING_STATE.handleValue;
								try {
									number = Long.parseLong(tag.substring("value".length()));
								} catch (NumberFormatException e) {
									System.err.println(tag);
									e.printStackTrace();
									number = -1;
								}
							}
						}
						bodyContentStream = null;
					}	break;
					case stateReadData:{
						bodyContentStream = new ByteArrayOutputStream();
						boolean[] passes = new boolean[4];
						for (int i=0; i<passes.length; i++) 
							passes[i] = false;
						while(!passes[3]){
							int ch = input.read();
							if(!passes[0]) {
								if(ch=='\r')
									passes[0] = true;
								else
									bodyContentStream.write(ch);
							} else {
								if(!passes[1]) {
									if(ch=='\n')
										passes[1] = true;
									else {
										bodyContentStream.write('\r');
										bodyContentStream.write(ch);
										passes[0] = false;
									}
								} else {
									if(!passes[2]) {
										if(ch=='-')
											passes[2] = true;
										else {
											bodyContentStream.write('\r');
											bodyContentStream.write('\n');
											bodyContentStream.write(ch);
											passes[0] = passes[1] = false;
										}
									} else {
										if(ch == '-')
											passes[3] = true;
										else {
											bodyContentStream.write('\r');
											bodyContentStream.write('\n');
											bodyContentStream.write('-');
											bodyContentStream.write(ch);
											passes[0] = passes[1] = passes[2] = false;
										}
									}
								}
							}
						}
//						System.out.println("Got Binary size " + bodyContentStream.size());
						
						switch(handling) {
						case handleJobType:
							jobType = new String(bodyContentStream.toByteArray());
							break;
						case handleBinaryFile:
							binaryFile = bodyContentStream.toByteArray();
							break;
						case handleAttribute:
							if(number!=-1) {
								String s =  new String(bodyContentStream.toByteArray());
								if(!s.isEmpty())
									Attribute.put(number,s);
								number = -1;
							}
							break;
						case handleValue:
							if(number!=-1) {
								String s =  new String(bodyContentStream.toByteArray());
								if(!s.isEmpty())
									Value.put(number, s);
								number = -1;
							}
							break;
						default:
							break;
						}						
						bodyContentStream = null;
						state = PARCING_STATE.stateBoundary;
					}	break;
					}					
					
				} while (!endOfMessage);
				
				if(binaryFile != null) {
					for(Long l:Attribute.keySet()) {
						if(Value.get(l)!=null) {
							newJob.AddDiscreteAttribute(Attribute.get(l), Value.get(l));
						}
					}
					newJob.AddDiscreteAttribute("JobType", jobType);
					myAgent.SubmitJob(newJob, binaryFile);
				}				
			}
			
//			System.out.println("Got " + size + " bytes of message");
//			System.out.println("Reading message done");			
			
			if(connectionType==HTTP_GET ) {
				if(file.equals("/")) {
//					System.out.println("StatusResponse");
					myAgent.AddTbfBehaviour(new StatusResponse(myAgent, client));
				}
				else {
					myAgent.AddTbfBehaviour(new StatusResponse(myAgent, client));
				}
			} else if(connectionType==HTTP_POST) {
				if(file.equals("/submit") && !boundary.isEmpty()){
//					System.out.println("UploadFileResponse");
					myAgent.AddTbfBehaviour(new StatusResponse(myAgent, client));
				}
				else {
//					System.out.println("StatusResponse");
					myAgent.AddTbfBehaviour(new StatusResponse(myAgent, client));
				}
			}
			
			System.out.println("ClientMessageReceieveBehaviour Terminated");
			
			
		} catch(Exception e){
			e.printStackTrace();
		}		
	}
	
	private class Error501Response extends OneShotBehaviour {
		private static final long serialVersionUID =1L;
		Socket client;
		public Error501Response(Agent a, Socket s){
			super(a);
			client = s;		
		}
		@Override
		public void action() {
			try {
				PrintStream output = new PrintStream(client.getOutputStream());
				output.print("HTTP/1.0 501 Not Implemented" + EOL);
				output.print("Server: tw.idv.ctfan.cloud.middleware.HTTPServerBehaviour" + EOL);
				output.print("Date: " + new Date() + EOL);
				output.print("Content-Type: text/html" + EOL);
				output.print(EOL);
				output.print("<HTML><HEAD><TITLE>Error 501 Not Implemented</TITLE></HEAD>");
				output.print("<BODY><H2>501 Not Implemented</H2>");
				output.print("<p />" + "Error" + "</BODY></HTML>");
				output.flush();
			} catch (Exception e) {
				System.err.println("HTTPServerBehaviour::Error501Response::action::out Error");
				e.printStackTrace();
			} finally {
				try {
					client.close();
				} catch (Exception e) {
					System.err.println("HTTPServerBehaviour::Error501Response::client closing error");
					e.printStackTrace();
				}
			}
		}		
	}	
	
	private class StatusResponse extends OneShotBehaviour {
		private static final long serialVersionUID = 1L;
		Socket client;
		private static final String styleRight = "border: 3px black solid; float: right; display:inline-block; margin:5px; width:35%";
		private static final String styleLeft = "border: 3px black solid; float: left; display:inline-block; margin:5px; width:60%";
		
		public StatusResponse(Agent a, Socket s) {
			super(a);
			client = s;
		}

		@Override
		public void action() { synchronized(policy) {
			try {
//				System.out.println("StatusResponse Start");
				PrintStream output = new PrintStream(client.getOutputStream());
				
				//=====
				
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
						
						// Auto refresh
//						"<meta http-equiv=\"refresh\" content=\"15\" />" +
						
						// Script to change job
//						"<script language=\"JavaScript\">function onSelectChange(){" +
//						"var j = document.getElementById(\"javaParameter\");"+
//						"var h = document.getElementById(\"hadoopParameter\");"+
//						"j.style.visibility=\"hidden\";"+
//						"h.style.visibility=\"hidden\";"+
//						"var value = document.getElementById(\"jobType\").selectedIndex;"+
//						"if(value==0)"+
//						"{j.style.visibility=\"visible\";}"+
//						"else if(value==1)"+
//						"{h.style.visibility=\"visible\";}}"+
//						"</script>"+
						"</HEAD>");
				
				// Start of Body
				output.print("<BODY>");
								
				// LOGOS
				output.print("<DIV>");
//				output.print("<H1><img src=\"http://dmclab.csie.ntpu.edu.tw/web/media/logo_action.gif\" />Hybrid Cloud Information Viewer</H1>");
				output.print("<H1 style=\"font-size:64px; text-align:center; margin:5px\"><IMG style=\"width:64px\" src=\"http://120.126.145.102/mtp/DMCL_logo.gif\" />Federated Cloud Information Viewer</H1>");
				output.print("<h3 style=\"text-align:right; margin:3px\">Copyright: C.T.Fan</h3>");
				output.print("<h3 style=\"margin:3px\">Uptime: "+ m_initTime.toString() +"</h3>");
				output.print("</DIV>");
				output.print("<HR/>");

				// Submit form
				output.print("<DIV style=\"" + styleRight + "\">");
				output.print("<H1>Submit Job</H3><BR/>");
				output.print("<FORM action=\"submit\" method=\"post\" enctype=\"multipart/form-data\">");
				
				output.print("Job Type: <select name=\"jobType\" id=\"jobType\">");
				for(JobType jt:policy.GetJobTypeList()) {
					output.print("<option value=\""+jt.getTypeName()+"\">" + jt.getTypeName() + "</option>");
				}				
				output.print("</select>");
				
				output.print("Binary File: <INPUT type=\"file\" name=\"binaryFile\" accept=\"application/java-archive\" /><br/>");
				
				for(int i=0; i<5; i++) {
					String s = 
						"Attribute" + i + ": <INPUT type=\"text\" name=\"attribute" + i + "\"/>" +
						"Value" + i + ": <INPUT type=\"text\" name=\"value" + i + "\"/>" + 
						"<BR />";
					output.print(s);
				}
				
				output.print("<INPUT type=\"submit\" value=\"submit\" />");
				output.print("</FORM>");
				output.print("</DIV>");
				

				// cluster+running list
				output.print("<DIV style=\"" + styleLeft + "\">");
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
					if(cn.GetMachineList().get(0).vmController.masterType != VMController.VirtualMachineType.Private) continue;
					long remainTime=0;
					for(JobNode jn : policy.GetRunningJob()) {
						if(jn.runningCluster!=null&&jn.runningCluster==cn) {
							long time = jn.GetContinuousAttribute("PredictionTime");
							if(time<=0) time = 2000000;
							remainTime += time-jn.completionTime;
						}
					}
					output.print("<TR style=\"border-top:1px solid black\"><TD>" + cn.clusterName + "</TD>" +
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
					for(JobNode jn: policy.GetRunningJob()) {
						if(jn.runningCluster!=null&&jn.runningCluster==cn)
							output.print("<tr style=\"border-top:1px solid black\"><td>" + jn.UID + "</td>" +
									                                              "<td>" + jn.jobType.getTypeName() + "</td>" +
									                                              "<td>" + jn.GetContinuousAttribute("PredictionTime") + "</td>" +
									                                              "<td>" + jn.completionTime + "</td>" +
									                                              "<td>" + jn.deadline + "</tr>");
					}
					
					output.print("</table>");
					
					output.print("</td></tr>");
				}
				output.print("</TBODY>");
				
				output.print("</TABLE>");		
				output.print("</DIV>");
				

				

				output.print("<DIV style=\"" + styleLeft + "\">");
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
					if(cn.GetMachineList().get(0).vmController.masterType != VMController.VirtualMachineType.Public) continue;
					long remainTime=0;
					for(JobNode jn : policy.GetRunningJob()) {
						if(jn.runningCluster!=null&&jn.runningCluster==cn) {
							long time = jn.GetContinuousAttribute("PredictionTime");
							if(time<=0) time = 2000000;
							remainTime += time-jn.completionTime;
						}
					}
					output.print("<TR style=\"border-top:1px solid black\"><TD>" + cn.clusterName + "</TD>" +
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
					for(JobNode jn: policy.GetRunningJob()) {
						if(jn.runningCluster!=null&&jn.runningCluster==cn)
							output.print("<tr style=\"border-top:1px solid black\"><td>" + jn.UID + "</td>" +
									                                              "<td>" + jn.jobType.getTypeName() + "</td>" +
									                                              "<td>" + jn.GetContinuousAttribute("PredictionTime") + "</td>" +
									                                              "<td>" + jn.completionTime + "</td>" +
									                                              "<td>" + jn.deadline + "</tr>");
					}
					
					output.print("</table>");
					
					output.print("</td></tr>");
				}
				output.print("</TBODY>");
				
				output.print("</TABLE>");		
				output.print("</DIV>");
				
				
				// waiting list

				output.print("<DIV style=\"" + styleLeft + "\">");
				output.print("<h1>Waiting Job List</h1>");
				output.print("<TABLE style=\"text-align:center; border-collapse:collapse; border:1px black solid; width:100%\">");
				
				output.print("<THEAD>" +
								"<TR style=\"border-top:1px black solid\"><TH style=\"width:34%\">Job Name</TH>" +
																	     "<TH style=\"width:33%\">Job Type</TH>" +
																	     "<TH style=\"width:33%\">Deadline</TH>" +
							 "</THEAD>");
				
				for(JobNode jn:policy.GetWaitingJob()) {
					output.print("<tr style=\"border-top:1px solid black\"><td>" + jn.UID + "</td>" +
																		  "<td>" + jn.jobType.getTypeName() + "</td>" +
																		  "<td>" + jn.deadline + "</td>" +
																		  "</tr>");
				}
				
				output.print("</TBODY>");
				
				output.print("</TABLE>");
				
				output.print("</DIV>");
				
				// finish list			

				output.print("<DIV style=\"" + styleLeft + "\">");
				output.print("<h1>Finished Job List</h1>");
				output.print("<TABLE style=\"text-align:center; border-collapse:collapse; border:1px black solid; width:100%\">");
				
				output.print("<THEAD>" +
								"<TR style=\"border-top:1px black solid\"><TH style=\"width:15%\">Job Name</TH>" +
																		 "<TH style=\"width:15%\">Job Type</TH>" +
																		 "<TH style=\"width:15%\">Finished Time</TH>" +
																		 "<TH style=\"width:15%\">Prediction Time</TH>" +
																		 "<TH style=\"width:15%\">Start Time</TH>" +
																		 "<TH style=\"width:15%\">Finish Time</TH></TR>"+
							 "</THEAD>");
				
				for(JobNode jn:policy.GetFinishJob()) {
					output.print("<tr style=\"border-top:1px solid black\"><td>" + jn.UID + "</td>" +
																		  "<td>" + jn.jobType.getTypeName() + "</td>" +
																		  "<td>" + jn.completionTime + "</td>" +
																		  "<td>" + jn.GetContinuousAttribute("PredictionTime") + "</td>" +
																		  "<td>" + jn.startTime + "</td>" +
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
				
				client.close();
				//=====
				
			} catch(Exception e) {
				System.err.println("Error in tw.idv.ctfan.cloud.middleware.HTTPServerBehaviour.StatusResponse");
				e.printStackTrace();
			} 
		} }
	}	
}
