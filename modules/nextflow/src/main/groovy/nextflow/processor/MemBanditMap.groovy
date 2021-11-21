package nextflow.processor

import groovy.util.logging.Slf4j

@Slf4j
@Singleton(lazy = true)
class MemBanditMap {
    Map<String,MemoryBandit> bandits = [:];

    synchronized MemoryBandit getBandit(long minMem, long maxMem, long currentMem, String task_name, boolean withLogs){
        MemoryBandit b = bandits.get(task_name);
//        Date now = new Date()
        if (!b) {
            b = new MemoryBandit(minMem,maxMem,currentMem,task_name,withLogs)
            bandits.put(task_name,b)
//            log.info("Creating new bandit: $b for $task_name at ${now.toString()}")
        } else {
//            log.info("Found existing bandit: $b for $task_name at ${now.toString()}")
        }
        return b
    }

}
