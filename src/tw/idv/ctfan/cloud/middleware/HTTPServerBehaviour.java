package tw.idv.ctfan.cloud.middleware;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;

import tw.idv.ctfan.cloud.middleware.policy.Policy;
import tw.idv.ctfan.cloud.middleware.policy.data.ClusterNode;
import tw.idv.ctfan.cloud.middleware.policy.data.JobNodeBase;
import tw.idv.ctfan.cloud.middleware.policy.data.VMMasterNode;

import jade.core.Agent;
import jade.core.behaviours.CyclicBehaviour;
import jade.core.behaviours.OneShotBehaviour;


public class HTTPServerBehaviour extends CyclicBehaviour {
	private static final long serialVersionUID = 1L;
	private final byte[] EOL = { (byte)'\r', (byte)'\n' };
	ServerSocket server;
	Policy policy;
	
	static final Date m_initTime = new Date();
	
	private static final int HTTP_POST = 0x301;
	private static final int HTTP_GET  = 0x302;

	public HTTPServerBehaviour(AdministratorAgent agent, Policy policy) {
		super(agent);
		this.policy = policy;
		
		try {
			server = new ServerSocket(50071);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void action() {
		try {
			Socket client = server.accept();
			
			boolean endOfHeader = false;		
			
			BufferedInputStream input = new BufferedInputStream(client.getInputStream());
			
			int connectionType = 0;
			long contentLength = 0;
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
						myAgent.addBehaviour(new Error501Response(myAgent, client));
						return;
					}
				} else {
					if(info[0].matches("Content-Length:")) {
						contentLength = Long.parseLong(info[1]);
					}
					else if(info[0].matches("Content-Type:")) {
						if(info[1].matches("multipart/form-data;")) {
							String bound[] = info[2].split("=");
							if(bound.length == 2 && bound[0].matches("boundary"))
								boundary = bound[1];
							else {
								myAgent.addBehaviour(new Error501Response(myAgent, client));
								return;
							}
						}
						else {
							myAgent.addBehaviour(new Error501Response(myAgent, client));
							return;
						}							
					}
				}				
			} while(!endOfHeader);
			
			System.out.println("Reading header done");
			
			// Read body content
			ByteArrayOutputStream bodyContentStream;
			int size = 0;
			
			if(connectionType==HTTP_POST) {
				boolean endOfMessage = false;
				int state = 0x301;
				final int STATE_BOUNDARY = 0x301;
				final int STATE_DESCRIPTOR = 0x302;
				final int STATE_READ_DATA = 0x303;
				
				do {
					switch (state) {
					case STATE_BOUNDARY: {
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
							System.out.println("--"+boundary);
							state = STATE_DESCRIPTOR;
						} else if(b.compareTo("--"+boundary+"--")==0){
							System.out.println("--"+boundary+"--");
							endOfMessage = true;
						} else if(b.compareTo(boundary)==0){
							System.out.println(boundary);
							state = STATE_DESCRIPTOR;
						} else if(b.compareTo(boundary+"--")==0){
							System.out.println(boundary+"--");
							endOfMessage = true;
						}
						bodyContentStream = null;
					}	break;
					case STATE_DESCRIPTOR:{
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
							state = STATE_READ_DATA;
						else
							System.out.println(new String(bodyContentStream.toByteArray()));
						bodyContentStream = null;
					}	break;
					case STATE_READ_DATA:{
						bodyContentStream = new ByteArrayOutputStream();
						boolean[] passes = new boolean[4];
						for (boolean b:passes) 
							b = false;
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
						System.out.println("Got Binary size " + bodyContentStream.size());
						bodyContentStream = null;
						state = STATE_BOUNDARY;
					}	break;
					}					
					
				} while (!endOfMessage);
			}
			
			System.out.println("Got " + size + " bytes of message");
			System.out.println("Reading message done");
			
			
			if(connectionType==HTTP_GET ) {
				if(file.equals("/")) {
					System.out.println("StatusResponse");
					myAgent.addBehaviour(new StatusResponse(myAgent, client));
				}
//				else {
//					myAgent.addBehaviour(new GetFileResponse(myAgent, client));
//				}
			} else if(connectionType==HTTP_POST) {
				if(file.equals("/submit") && !boundary.isEmpty()){
					System.out.println("UploadFileResponse");
					myAgent.addBehaviour(new UploadFileResponse(myAgent, client, new byte[0], boundary));
				}
				else {
					System.out.println("StatusResponse");
					myAgent.addBehaviour(new StatusResponse(myAgent, client));
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
	
	private class UploadFileResponse extends OneShotBehaviour {
		private static final long serialVersionUID =1L;
		Socket client;
		byte[] inputByte;
		byte[] boundaryByte;
		String boundary;
		
		public UploadFileResponse(Agent a, Socket s, byte[] in, String bound){
			super(a);
			client = s;
			inputByte = in.clone();
			boundary = bound;
			boundaryByte = boundary.getBytes();
		}

		@Override
		public void action() {
			System.out.println("Start UploadFileResponse");
			try {
				System.out.println("Boundary: " + boundary);
				
//				int start, end = 0;
//				int line=0;
//				boolean bound = true;
//				
//				for(int i=0; i<inputByte.length; i++) {
//					if(bound = true) {
//						for(int j=0; j<inputB; j++) {
//							
//						}
//					}
//				}
				
								
			} catch (Exception e) {
				e.printStackTrace();
			}
			finally {
				myAgent.addBehaviour(new StatusResponse(myAgent, client));
			}
			
		}
	}
	
	private class StatusResponse extends OneShotBehaviour {
		private static final long serialVersionUID = 1L;
		Socket client;
		
		public StatusResponse(Agent a, Socket s) {
			super(a);
			client = s;
		}

		@Override
		public void action() {
			try {
				System.out.println("StatusResponse Start");
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
						//"<meta http-equiv=\"refresh\" content=\"5\" />" +
						"<script language=\"JavaScript\">function onSelectChange(){" +
						"var j = document.getElementById(\"javaParameter\");"+
						"var h = document.getElementById(\"hadoopParameter\");"+
						"j.style.visibility=\"hidden\";"+
						"h.style.visibility=\"hidden\";"+
						"var value = document.getElementById(\"jobType\").selectedIndex;"+
						"if(value==0)"+
						"{j.style.visibility=\"visible\";}"+
						"else if(value==1)"+
						"{h.style.visibility=\"visible\";}}"+
						"</script>"+
						""+
						""+
						""+
						""+
						""+
						""+
						""+
						""+
						""+
						"</HEAD>");
				
				// Start of Body
				output.print("<BODY>");
								
				// LOGOS
				output.print("<DIV>");
				//output.print("<H1><img src=\"http://dmclab.csie.ntpu.edu.tw/web/media/logo_action.gif\" />Hybrid Cloud Information Viewer</H1>");
				output.print("<div style=\"float:right;\"><h3>Copyright: C.T.Fan</h3></div>");
				output.print("<h3>Uptime: "+ m_initTime.toString() +"</h3>");
				output.print("</DIV>");
				output.print("<HR/>");

				// Submit form
				output.print("<DIV style=\"border: 1px black solid;\">");
				output.print("<H3>Submit Job</H3><BR/>");
				output.print("<FORM action=\"submit\" method=\"post\" enctype=\"multipart/form-data\">");
				output.print("Job Type: <select name=\"jobType\" id=\"jobType\" onchange=\"onSelectChange()\"><option value=\"Java\">Java</option><option value=\"Hadoop\">Hadoop</option></select>");
				output.print("Binary File: <INPUT type=\"file\" name=\"binaryFile\" accept=\"application/java-archive\" /><br/>");
				output.print("<div id=\"javaParameter\">Parameter: <INPUT type=\"text\" name=\"parameter\" /></div>");
				output.print("<div id=\"hadoopParameter\" style=\"visibility:hidden;\">Input Folder: <INPUT type=\"text\" name=\"hadoopInput\" /> Output Folder: <INPUT type=\"text\" name=\"hadoopOutput\" /></div>");
				output.print("<INPUT type=\"submit\" value=\"submit\" />");
				output.print("</FORM>");
				output.print("</DIV>");
				output.print("<HR/>");
				

				// cluster+running list
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
				
				client.close();
				//=====
				
			} catch(Exception e) {
				System.err.println("Error in tw.idv.ctfan.cloud.middleware.HTTPServerBehaviour.StatusResponse");
				e.printStackTrace();
			}
		}
		
	}
	
}
