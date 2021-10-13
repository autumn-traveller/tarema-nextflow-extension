package nextflow.processor

import groovy.util.logging.Slf4j

@Slf4j
@Singleton(lazy = true)
class AgentMap {
    //TODO: make this a map of the memory bandit!
    Map<String,QAgent> bandits = [:];

    synchronized QAgent getBandit(int initialCpu, int maxCpu, long initialMem, String taskName, boolean withLogs){
        QAgent b = bandits.get(task_name);
//        Date now = new Date()
        if (!b) {
            b = new QAgent(initialCpu,maxCpu,initalMem,task_name,withLogs)
            bandits.put(task_name,b)
//            log.info("Creating new bandit: $b for $task_name at ${now.toString()}")
        } else {
//            log.info("Found existing bandit: $b for $task_name at ${now.toString()}")
        }
        return b
    }

}
