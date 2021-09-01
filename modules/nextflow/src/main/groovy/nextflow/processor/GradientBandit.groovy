package nextflow.processor

import groovy.sql.Sql
import groovy.util.logging.Slf4j
import nextflow.TaskDB

@Slf4j
class GradientBandit {
    int maxCpu
    double[] cpuPreferences
    double[] cpuProbabilities
    double cpuAvgReward
    long maxMem
    long minMem
    int memChunkSize
    int numChunks
    double[] memoryPreferences
    double[] memoryProbabilities
    double memoryAvgReward
    double stepSizeCpu
    double stepSizeMem
    String taskName
    int numRuns

    public GradientBandit(int maxCpu, long minMem, long maxMem, int chunkSize, int numChunks, double stepSizeCpu, double stepSizeMem, String taskName){
        this.maxCpu = maxCpu
        this.minMem = minMem
        this.maxMem = maxMem
        this.memChunkSize = chunkSize
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

    public GradientBandit(int cpus, String taskName, double stepSize){
        this.taskName = taskName
        this.maxCpu = cpus
        this.stepSizeCpu = stepSize
        this.cpuPreferences = new double[maxCpu] // 0 to start
        this.cpuProbabilities = new double[maxCpu]
        for (i in 0..<maxCpu) {
            cpuProbabilities[i] = 1.0/((double) maxCpu) // initial probability is the same
        }
        this.cpuAvgReward = 0
        this.numRuns = 0
    }

    private void updateCpuProbabilities(){
        def s = 0
        for (i in 0..<maxCpu) {
            s += Math.exp(cpuPreferences[i])
        }
        for (i in 0..<maxCpu) {
            cpuProbabilities[i] = Math.exp(cpuPreferences[i]) / s
        }
    }

    boolean enable_logs = true

    private void logInfo(String var1, Object... var2){
        if(enable_logs){
            log.info(var1,var2)
        }
    }

    private void updateCpuPreferences(int cpus, float usage, int realtime){
        def r = reward(cpus, usage, realtime)
        logInfo("Task \"$taskName\": cpus alloc'd $cpus, cpu usage $usage, realtime $realtime -> reward $r (avg reward so far: $cpuAvgReward)\n")
        for (i in 0..<maxCpu) {
            if (i == cpus - 1){
                def oldval = cpuPreferences[i]
                cpuPreferences[i] = cpuPreferences[i] + stepSizeCpu * (r - cpuAvgReward) * (1 - cpuProbabilities[i])
                logInfo("Task \"$taskName\": update (allocd cpus) preference: cpuPreferences[$i] = $oldval + $stepSizeCpu * ($r - $cpuAvgReward) * (1 - ${cpuProbabilities[i]}) = ${cpuPreferences[i]}\n")
            } else {
                def oldval = cpuPreferences[i]
                cpuPreferences[i] = cpuPreferences[i] - stepSizeCpu * (r - cpuAvgReward) * (cpuProbabilities[i])
                logInfo("Task \"$taskName\": update rest preferences: cpuPreferences[$i] = $oldval - $stepSizeCpu * ($r - $cpuAvgReward) *  ${cpuProbabilities[i]} = ${cpuPreferences[i]}\n")

            }
        }

    }

    private void readPrevRewards() {
        logInfo("Searching SQL for Bandit $taskName")
        def sql = new Sql(TaskDB.getDataSource())
        def searchSql = "SELECT cpus,cpu_usage,realtime FROM taskrun WHERE task_name = (?)"
        sql.eachRow(searchSql,[taskName]) { row ->
            def cpus = (int) row.cpus
            def usage = (float) row.cpu_usage
            def realtime = (int) row.realtime
            logInfo("Task \"$taskName\": prefs and probabilities BEFORE: $cpuPreferences , $cpuProbabilities")
            updateCpuPreferences(cpus,usage,realtime)
            updateCpuProbabilities()
            logInfo("Task \"$taskName\": prefs and probabilities AFTER: $cpuPreferences , $cpuProbabilities")
        }
        sql.close()
        logInfo("Done with SQL for Bandit $taskName")
    }

    double reward(int cpuCount, float usage, int realtime) {
        // aim for 100% usage of the allocated cpus- overusage is considered equally bad as underusage
        // highest reward comes with precisely 100% usage of the available cpus and minimal runtime
        // reward is negative so we want to keep its absolute value small since we are using it with the exp() function
        // divide by 1000 because realtime measurements are inaccurate for tasks with runtimes smaller than 1s
        double r = -1 * (realtime/1000) * (1 + Math.abs(cpuCount - usage/100))
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
                return i + 1
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
        logInfo(s)
    }

    public int allocateCpu(){
        readPrevRewards()
        //logBandit()
        return pickCpu(Math.random())
    }

    public int allocateMem(){
        return 0
    }

}
