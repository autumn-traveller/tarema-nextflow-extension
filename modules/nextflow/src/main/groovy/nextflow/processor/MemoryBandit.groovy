package nextflow.processor

import groovy.sql.Sql
import groovy.util.logging.Slf4j
import nextflow.TaskDB
import nextflow.processor.TaskProcessor.RunType
import java.sql.SQLException

@Slf4j
class MemoryBandit {
    long maxMem
    long minMem
    long chunkSize
    int numChunks
    double[] memoryPreferences
    double[] memoryProbabilities
    double memoryAvgReward
    double stepSize
    String taskName
    int numRuns
    boolean withLogs
    boolean tooShort

    private static long toMega(m) { m >> 20 }

    private static String memPrint(long m){
        return "${toMega(m)} MB"
    }

    private void logInfo(String var1, Object... var2){
        if(withLogs){
            log.info("MemBandit \"$taskName\" $var1",var2)
        }
    }
    private void logError(String var1, Object... var2){
        def f = new File("errlog")
        f.append("MemBandit \"$taskName\": $var1\n")
        log.error("MemBandit \"$taskName\": $var1",var2)
    }

    public MemoryBandit(long minMem, long maxMem, long currentMem, String taskName, boolean withLogs){
        this.taskName = taskName
        this.withLogs = withLogs
        this.stepSize = 0.1 // perhaps 0.2 ?
        this.minMem = minMem
        this.maxMem = maxMem
        this.numChunks = 10 // possible allocations are 0.125 up to 1.25 times the default config
        this.chunkSize = currentMem >> 3 // memory chunks are 1/8th of the default configuration
        if (chunkSize <= minMem){
            chunkSize = chunkSize << 1
        }
        if(10*chunkSize >= maxMem){
            numChunks = 8
        }
//        if(checkTooShort()){
//            return
//        }
        this.memoryPreferences = new double[numChunks] // 0 to start
        this.memoryProbabilities = new double[numChunks]
        for (i in 0..<numChunks) {
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

    private void updatePreferences(long rss, long mem, long realtime){
        def r = reward(rss, mem, realtime)
        int memIndex = (mem / chunkSize) - 1
        if (!(memIndex in 0..numChunks)){
            log.warn("Invalid memIndex $memIndex for ${memPrint(mem)} and chunksize $chunkSize")
            memIndex = Math.round(mem/chunkSize) - 1
            if (!(memIndex in 0..numChunks)){
                logError("Invalid memIndex even when rounding: $memIndex for ${memPrint(mem)} and chunksize $chunkSize")
                return
            }
        }
        logInfo("memory alloc'd ${memPrint(mem)}, mem usage ${memPrint(rss)} (${rss*100/mem} %) -> reward $r\n")
        for (i in 0..<numChunks) {
            if (i == memIndex ){
                def oldval = memoryPreferences[i]
                memoryPreferences[i] = oldval + stepSize * (r - memoryAvgReward) * (1 - memoryProbabilities[i])
                logInfo("update rest preferences: memPreferences[$i] = $oldval + $stepSize * ($r - $memoryAvgReward) *  (1 - ${memoryProbabilities[i]}) = ${memoryPreferences[i]}\n")
            } else {
                def oldval = memoryPreferences[i]
                memoryPreferences[i] = oldval - stepSize * (r - memoryAvgReward) * (memoryProbabilities[i])
                logInfo("update rest preferences: memPreferences[$i] = $oldval - $stepSize * ($r - $memoryAvgReward) *  ${memoryProbabilities[i]} = ${memoryPreferences[i]}\n")
            }
        }

    }

    private void readPrevRewards() {
        logInfo("Searching SQL for Bandit $taskName")
        def sql = new Sql(TaskDB.getDataSource())
        def searchSql = "SELECT peak_rss,memory,realtime FROM taskrun WHERE task_name = (?)"
        sql.eachRow(searchSql,[taskName]) { row ->
            def rss = (long) row.peak_rss
            def mem = (long) row.memory
            def realtime = (long) row.realtime
            logInfo("probabilities BEFORE: $memoryProbabilities")
            updatePreferences(rss,mem,realtime)
            updateProbabilities()
            logInfo("probabilities AFTER: $memoryProbabilities")
        }
        sql.close()
        logInfo("Done with SQL for Bandit $taskName")
    }

    double reward(long rss, long memory, long realtime) {
        double unusedChunks = ((double) Math.max((memory - rss),0)) / ((double) chunkSize)
        double r = (rss != memory+1) ? -1*unusedChunks : -2*memory/chunkSize // if we were killed by the oom killer we receive -2 * the memory allocated
        memoryAvgReward = (numRuns * memoryAvgReward + r)/(numRuns + 1)
        numRuns++
        return r
    }

    void logBandit(){
        def s = ""
        s += "Bandit $taskName\n"
        for (i in 0..<numChunks) {
            s += "Action ${memPrint((i+1)*chunkSize)} : Preference ${memoryPreferences[i]} Probability ${memoryProbabilities[i]}\n"
        }
        s += "$memoryAvgReward"
        logInfo(s)
    }

    public synchronized long allocateMem(int failcount, RunType runtype,long currentConfig){
        if(tooShort){
            return 8 * chunkSize // default config
        }
        readPrevRewards()
        long r = 0
        def rand = Math.random()
        double pdf = 0
        for (i in 0..<numChunks) {
            pdf += memoryProbabilities[i]
            if (rand <= pdf){
                r = (i + 1)*chunkSize
                break
            }
        }
        if(r == 0) {
            logError("$taskName Bandit couldnt pick a memory allocation, are the probabilities okay? ($memoryProbabilities) ... defaulting to original config")
            return 8 * chunkSize // default config
        }
        if (runtype == RunType.RETRY && r <= currentConfig){
            logError("$taskName Bandit was killed with ${memPrint(currentConfig)} but the bandit picked ${memPrint(r)} as new config. Now returning either double that or the original config")
            if((r * 2) <= currentConfig || r/chunkSize >= 0.5) {
                return 8 * chunkSize
            } else {
                return r*2
            }
        }
        return r
    }

    private boolean checkTooShort(){
        tooShort = false
        try{
            def sql = new Sql(TaskDB.getDataSource())
            def searchSql = "SELECT COUNT(realtime), AVG(realtime) FROM taskrun WHERE task_name = (?)" // "and rl_active = false"
            sql.eachRow(searchSql,[taskName]) { row ->
                if (row.count && row.count as int >= 5 && row.avg && row.avg as int < 1000){
                    tooShort = true // these tasks are too short for the nextflow metrics to be accurate
                }
            }
            sql.close()
        } catch (SQLException sqlException) {
            logError("There was an error: " + sqlException)
        }
        return tooShort
    }
}
