package nextflow.processor

import groovy.util.logging.Slf4j

@Slf4j
@Singleton(lazy = true)
class AgentMap {
    Map<String,QAgent> bandits = [:];

    synchronized QAgent getBandit(int initialCpu, int maxCpu, String taskName, String command, String runName, boolean withLogs){
        QAgent b = bandits.get(taskName);
//        Date now = new Date()
        if (!b) {
            b = new QAgent(initialCpu,maxCpu,taskName,command,runName,withLogs)
            bandits.put(taskName,b)
//            log.info("Creating new bandit: $b for $task_name at ${now.toString()}")
        } else {
//            log.info("Found existing bandit: $b for $task_name at ${now.toString()}")
        }
        return b
    }

}
