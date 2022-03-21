package nextflow.processor

import groovy.util.logging.Slf4j

@Slf4j
@Singleton(lazy = true)
class MemBanditMap {
    Map<String,MemoryBandit> bandits = [:];

    synchronized MemoryBandit getBandit(long initialConfig, int numChunks, String taskName, String cmd, boolean withLogs){
        MemoryBandit b = bandits.get(taskName);
//        Date now = new Date()
        if (!b) {
            b = new MemoryBandit(initialConfig,numChunks,taskName,cmd,withLogs)
            bandits.put(taskName,b)
//            log.info("Creating new bandit: $b for $taskName at ${now.toString()}")
        }
        return b
    }

}
