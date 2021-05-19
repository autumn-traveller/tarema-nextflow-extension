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

    private void updatePreferences() {
        log.info("Searching SQL for Bandit $taskName")
        def sql = new Sql(TaskDB.getDataSource())
        def searchSql = "SELECT cpu,cpus,realtime FROM taskrun WHERE task_name = (?)"
        def rowNum = 0
        sql.eachRow(searchSql,[taskName]) { row ->
            def cpus = (int) row.cpus
            def usage = (float) row.cpu
            def time = (int) row.realtime
            def r = reward(cpus, usage, time)
            log.info("Task \"${taskName}\", row ${++rowNum} : cpu usage ${usage}, cpus alloc'd ${cpus}, realtime ${time} -> reward ${r}")
            for (i in 0..<maxCpu) {
                def s = ""
                s += "action ${i+1} cpus- BEFORE: pref ${cpuPreferences[i]}, prob ${cpuProbabilities[i]}, avg reward ${cpuAvgReward}\n"
                if(i == cpus - 1){
                    cpuPreferences[cpus - 1] = cpuPreferences[cpus - 1] + stepSizeCpu * (r - cpuAvgReward) * (1 - cpuProbabilities[cpus - 1])
                } else {
                    cpuPreferences[i] = cpuPreferences[i] - stepSizeCpu * (r - cpuAvgReward) * (cpuProbabilities[i])
                }
                s += "action ${i+1} cpus- AFTER: pref ${cpuPreferences[i]}, prob ${cpuProbabilities[i]}, avg reward ${cpuAvgReward}"
                log.info(s)
            }
        }
        sql.close()
        log.info("Done with SQL for Bandit $taskName")
    }

    int reward(int cpuCount, float usage, int realtime) {
        // aim for 100% usage of the allocated cpus- overusage is treated as equally bad as underusage
        // highest reward comes with precisely 100% cpu usage and minimal runtime
        // reward is negative so we want to keep its absolute value small since we are using it with the exp() function
        double r = -1 * realtime * (1 + Math.abs(cpuCount - usage/100))
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
            if(rand <= pdf){
                ret = i + 1
                break
            }
        }
        return ret
    }

    void logBandit(){
        def s = ""
        s += "Bandit ${taskName}\n"
        for (i in 0..<maxCpu) {
            s += "Action ${i+1} cpus: Preference ${cpuPreferences[i]} Probability ${cpuProbabilities[i]}\n"
        }
        s += "$cpuAvgReward"
        log.info(s)
    }

    public int allocateCpu(){
        updatePreferences()
        updateCpuProbabilities()
        logBandit()
        return pickCpu(Math.random())
    }

    public int allocateMem(){
        return 0
    }

}
