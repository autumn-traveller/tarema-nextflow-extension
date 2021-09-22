package nextflow.processor

import java.sql.SQLException
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

    public GradientBandit(int cpus, String taskName){
        this.taskName = taskName
        this.maxCpu = cpus
        this.stepSizeCpu = 0.025
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

    boolean enable_logs = false

    private void logInfo(String var1, Object... var2){
        if(enable_logs){
            log.info(var1,var2)
        }
    }

    private void updateCpuPreferences(int cpus, float usage){
        def r = reward(cpus, usage)
        logInfo("Task \"$taskName\": cpus alloc'd $cpus, cpu usage $usage-> reward $r (avg reward so far: $cpuAvgReward)\n")
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
        def searchSql = "SELECT cpus,cpu_usage FROM taskrun WHERE task_name = (?) and rl_active = true"
        sql.eachRow(searchSql,[taskName]) { row ->
            def cpus = (int) row.cpus
            def usage = (float) row.cpu_usage
            logInfo("Task \"$taskName\": prefs and probabilities BEFORE: $cpuPreferences , $cpuProbabilities")
            updateCpuPreferences(cpus,usage)
            updateCpuProbabilities()
            logInfo("Task \"$taskName\": probabilities AFTER: $cpuProbabilities")
        }
        sql.close()
        logInfo("Done with SQL for Bandit $taskName")
    }

    double reward(int cpuCount, float usage) {
        // reward function is maximized when cpuUsage = 115% of the allocated cpus, we dont want to underuse or overclock them
        // Therefore 110% and 120% usage both yield the same reward -> -5
        // since the usage field normally needs to be divided by 100 first, usage divided cpuCount converts directly to a percentage
        double r = -1 * Math.abs(115 - usage/cpuCount)
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
