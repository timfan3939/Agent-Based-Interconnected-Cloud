pscp -pw unigrid -r "C:\Users\C.T.Fan\workspace\Jade - MP Middleware\bin\tw" hadoop@10.133.200.201:/home/hadoop/ctfan/jade/jar
:pscp -pw unigrid -r "C:\Users\C.T.Fan\workspace\Jade - MP Middleware\bin\tw" hadoop@10.133.200.202:/home/hadoop/ctfan/jade/jar
:pscp -pw unigrid -r "C:\Users\C.T.Fan\workspace\Jade - MP Middleware\bin\tw" hadoop@10.133.200.203:/home/hadoop/ctfan/jade/jar
:pscp -pw unigrid -r "C:\Users\C.T.Fan\workspace\Jade - MP Middleware\bin\tw" hadoop@10.133.200.204:/home/hadoop/ctfan/jade/jar
:pscp -pw unigrid -r "C:\Users\C.T.Fan\workspace\Jade - MP Middleware\bin\tw" hadoop@10.133.200.205:/home/hadoop/ctfan/jade/jar
:pscp -pw unigrid -r "C:\Users\C.T.Fan\workspace\Jade - MP Middleware\bin\tw" hadoop@10.133.200.206:/home/hadoop/ctfan/jade/jar
:pscp -pw unigrid -r "C:\Users\DMCLAB\workspace\Jade - MP Middleware\bin\tw" hadoop@10.133.200.6:/home/hadoop/ctfan/jade/jar

:-gui -name 120.126.145.114:1099/JADE  -services jade.core.mobility.AgentMobilityService;jade.core.migration.InterPlatformMobilityService -jade_core_messaging_MessageManager_maxqueuesize 40000000 CloudAdmin:tw.idv.ctfan.cloud.middleware.AdministratorAgent;DecisionAdmin:tw.idv.ctfan.cloud.middleware.ReconfigurationDecisionAgent;MigrationAdmin:tw.idv.ctfan.cloud.middleware.ServiceMigrationAgent
:-Xmx1024M -Xms128M

time /t