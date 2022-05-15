package nextflow.processor

import groovy.util.logging.Slf4j

@Slf4j
@Singleton(lazy = true)
class MemAgentMap {
    Map<String,MemAgent> agents = [:];

    synchronized MemAgent getAgent(long initialConfig, int numChunks, String taskName, String command, String runName, boolean withLogs){
        MemAgent r = agents.get(taskName);
        if (!r) {
            r = new MemAgent(initialConfig,numChunks,taskName,command,runName,withLogs)
            agents.put(taskName,r)
        }
        return r
    }

}
