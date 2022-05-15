package nextflow.processor

import groovy.util.logging.Slf4j

@Slf4j
@Singleton(lazy = true)
class QCpuMap {
    Map<String,QCpu> agents = [:];

    synchronized QCpu getAgent(int initialCpu, String taskName, String command, String runName, boolean withLogs){
        QCpu b = agents.get(taskName);
        if (!b) {
            b = new QCpu(initialCpu,taskName,command,runName,withLogs)
            agents.put(taskName,b)
        }
        return b
    }

}
