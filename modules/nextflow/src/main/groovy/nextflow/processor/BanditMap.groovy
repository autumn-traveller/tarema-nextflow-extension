package nextflow.processor

import groovy.util.logging.Slf4j

@Slf4j
@Singleton(lazy = true)
class BanditMap {
    //TODO: make this a map of the memory bandit!
    Map<String,GradientBandit> bandits = [:];

    synchronized GradientBandit getBandit(int cpus, String task_name, boolean withLogs){
        GradientBandit b = bandits.get(task_name);
//        Date now = new Date()
        if (!b) {
            b = new GradientBandit(cpus,task_name,withLogs)
            bandits.put(task_name,b)
//            log.info("Creating new bandit: $b for $task_name at ${now.toString()}")
        } else {
//            log.info("Found existing bandit: $b for $task_name at ${now.toString()}")
        }
        return b
    }

}
