package nextflow.processor

import groovy.sql.Sql
import groovy.util.logging.Slf4j
import nextflow.TaskDB

@Slf4j
class MemoryBandit {
    int maxCpu
    double[] cpuPreferences
    double[] cpuProbabilities
    double cpuAvgReward
    long maxMem
    long minMem
    int chunkSize
    int numChunks
    double[] memoryPreferences
    double[] memoryProbabilities
    double memoryAvgReward
    double stepSizeCpu
    double stepSizeMem
    String taskName
    int numRuns

    public MemoryBandit(int maxCpu, long minMem, long maxMem, int chunkSize, int numChunks, double stepSizeCpu, double stepSizeMem, String taskName){
        this.maxCpu = maxCpu
        this.minMem = minMem
        this.maxMem = maxMem
        this.chunkSize = chunkSize
        this.numChunks = numChunks
        this.stepSizeCpu = stepSizeCpu
        this.stepSizeMem = stepSizeMem
        this.taskName = taskName
        this.cpuPreferences = new double[maxCpu]
        this.cpuProbabilities = new double[maxCpu]
        for (i in 0..<maxCpu) {
            cpuProbabilities[i] = 1/((double)maxCpu)
        }
        this.cpuAvgReward = 0
        this.memoryPreferences = numChunks >= 0 ? new double [numChunks] : null
        this.memoryProbabilities = numChunks >= 0 ? new double [numChunks] : null
        for (i in 0..<numChunks) {
            memoryProbabilities[i] = 1.0/((double)numChunks)
        }
        this.memoryAvgReward = 0
        numRuns = 0
    }

    public MemoryBandit(long minMem, long maxMem, int chunkSize, String taskName, double stepSize){
        this.taskName = taskName
        this.stepSizeMem = stepSize
        this.minMem = minMem
        this.maxMem = maxMem
        this.chunkSize = chunkSize
        this.numChunks = (maxMem - minMem) / chunkSize
        this.memoryPreferences = new double[numChunks] // 0 to start
        this.memoryProbabilities = new double[numChunks]
        for (i in 0..<maxCpu) {
            memoryProbabilities[i] = 1.0/((double) numChunks) // initial probability is the same
        }
        this.memoryAvgReward = 0
        this.numRuns = 0
    }

    private void updateProbabilities(){
        def s = 0
        for (i in 0..<numChunks) {
            s += Math.exp(memoryPreferences[i])
        }
        for (i in 0..<numChunks) {
            memoryProbabilities[i] = Math.exp(memoryPreferences[i]) / s
        }
    }

    private void updatePreferences(long memIndex, float usage, int realtime){
        def r = reward(cpus, usage, realtime)
        log.info("Task \"$taskName\": memory alloc'd ${memIndex*chunkSize + minMem}, mem usage $usage, realtime $realtime -> reward $r\n")
        for (i in 0..<numChunks) {
            if (i == memIndex ){
                memoryPreferences[i] = memoryPreferences[i] + stepSizeMem * (r - memoryAvgReward) * (1 - memoryPreferences[cpus - 1])
            } else {
                memoryPreferences[i] = memoryPreferences[i] - stepSizeMem * (r - memoryAvgReward) * (memoryPreferences[i])
            }
        }

    }

    private void readPrevRewards() {
        // TODO
        log.info("Searching SQL for Bandit $taskName")
        def sql = new Sql(TaskDB.getDataSource())
        def searchSql = "SELECT cpus,cpu_usage,realtime FROM taskrun WHERE task_name = (?)"
        sql.eachRow(searchSql,[taskName]) { row ->
            def cpus = (int) row.cpus
            def usage = (float) row.cpu_usage
            def realtime = (int) row.realtime
            log.info("Task \"$taskName\": prefs and probabilities BEFORE: $cpuPreferences , $cpuProbabilities")
            updatePreferences(cpus,usage,realtime)
            updateProbabilities()
            log.info("Task \"$taskName\": prefs and probabilities AFTER: $cpuPreferences , $cpuProbabilities")
        }
        sql.close()
        log.info("Done with SQL for Bandit $taskName")
    }

    int reward(int cpuCount, float usage, int realtime) {
        // aim for 100% usage of the allocated cpus- overusage is treated as equally bad as underusage
        // highest reward comes with precisely 100% cpu usage and minimal runtime
        // reward is negative so we want to keep its absolute value small since we are using it with the exp() function
        double r = -1 * realtime/60 * (1 + Math.abs(cpuCount - usage/100))
//        double r = -1 * cpuCount * realtime
        cpuAvgReward = (numRuns * cpuAvgReward + r)/(numRuns + 1)
        numRuns++
        return r
    }

    private int pickCpu(double rand){
        int ret = 0
        double pdf = 0
        for (i in 0..<maxCpu) {
            pdf += cpuProbabilities[i]
            if (rand <= pdf){
                return i*chunkSize + 1
            }
        }
        log.error("$taskName Bandit couldnt pick a cpu, are the probabilities okay? ($cpuProbabilities) ... defaulting to 1 cpu")
        return -1
    }

    void logBandit(){
        def s = ""
        s += "Bandit $taskName\n"
        for (i in 0..<maxCpu) {
            s += "Action ${i+1} cpus: Preference ${cpuPreferences[i]} Probability ${cpuProbabilities[i]}\n"
        }
        s += "$cpuAvgReward"
        log.info(s)
    }

    public int allocateCpu(){
        readPrevRewards()
        updateCpuProbabilities()
        logBandit()
        return pickCpu(Math.random())
    }

    public int allocateMem(){
        return 0
    }

}
