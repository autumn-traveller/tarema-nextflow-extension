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
    long initialConfig
    long safeMax
    int numChunks
    double[] memoryPreferences
    double[] memoryProbabilities
    double memoryAvgReward
    double stepSize
    String taskName
    String command
    int numRuns
    boolean withLogs
    boolean tooShort
    int lastTaskId

    private static long toMega(m) { m >> 20 }
    private static long toGiga(m) { m >> 30 }

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

    public MemoryBandit(long initialConfig, int numChunks, String taskName, String cmd, boolean withLogs){
        this.taskName = taskName
        this.initialConfig = initialConfig
        this.command = cmd.split("\\./evaluation_assets/nf-core/")[1].replace('/','')
        this.withLogs = withLogs
        this.stepSize = 0.1 // perhaps 0.2 or 0.05?
        this.numChunks = numChunks
        if (checkTooShort()){
            return
        }
        this.minMem = 7 << 20 // 6MB is the minimum memory value for docker
        if (!pollHistoricUsage()){
            this.maxMem = initialConfig
            this.safeMax = initialConfig
            this.minMem = 7 << 20
        } else {
            this.safeMax = 2*maxMem;
        }
        if (taskName in ['qualimap','damageprofiler','adapter_removal'] && minMem < (1 << 30)) { // add other tasks which use java and require at least 1GB of heap space as needed
            logInfo("task $taskName is in the list of java task with a 1GB min. heap space requirement")
            minMem = 1 << 30
            maxMem += (1 << 30)
        }
        this.chunkSize = Math.round((maxMem - minMem) / numChunks)
        logInfo("memory options for task $taskName : min ${memPrint(minMem)}, max ${memPrint(maxMem)}, chunkSize ${memPrint(chunkSize)}, safeMax = ${memPrint(safeMax)}, initialConfig = ${memPrint(initialConfig)}")
        this.numChunks += 2 // 2 extra "safety" states: safeMax and initialConfig
        this.memoryPreferences = new double[numChunks] // 0 to start
        this.memoryProbabilities = new double[numChunks]
        for (i in 0..<numChunks) {
            memoryProbabilities[i] = 1.0/((double) numChunks) // initial probability is the same
        }
        this.memoryAvgReward = 0
        this.numRuns = 0
        this.lastTaskId = 0
    }

    private boolean pollHistoricUsage(){
        try {
            def sql = new Sql(TaskDB.getDataSource())
            def searchSql = "SELECT AVG(peak_rss), STDDEV(peak_rss), MIN(peak_rss), MAX(peak_rss) FROM taskrun WHERE task_name = (?) and wf_name like (?) and rl_active = false"
            sql.eachRow(searchSql,[taskName,"%${this.command}%".toString()]) { row ->
                long avg = row[0] as long
                long stddev = row[1] as long
                long min = row[2] as long
                long max = row[3] as long

                logInfo("historic data for task $taskName : avg ${memPrint(avg)} stddev ${memPrint(stddev)} min ${memPrint(min)} max ${memPrint(max)}")

                if (max < min) {
                    logInfo("historic data makes no sense, max ${memPrint(max)} is less than the minimum ${memPrint(min)}")
                    return false
                }
                this.minMem = avg - Math.abs(stddev) > this.minMem ? avg - Math.abs(stddev) : this.minMem
                if (max <= this.minMem) {
                    logInfo("max is less than the adjusted minimum ${memPrint(this.minMem)}")
                    this.maxMem = this.minMem * 2
                } else {
                    this.maxMem = max * 2
                }
            }
            sql.close()
        } catch (SQLException sqlException) {
            logError("There was an sql error when polling historic data: " + sqlException)
            return false
        }
        return true
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

    private void updatePreferences(long rss, long mem){
        int memIndex = ((mem - minMem)/ chunkSize) - 1
        if (mem == safeMax) {
            memIndex = numChunks - 2
        } else if (mem == initialConfig) {
            memIndex = numChunks - 1
        }
        if (!(memIndex in 0..<numChunks)){
            memIndex = Math.round(mem/chunkSize) - 1
            if (!(memIndex in 0..<numChunks)){
                if (mem % initialConfig != 0 && mem % maxMem != 0) {
                    logError("Unusual memIndex even when rounding: $memIndex for ${memPrint(mem)}, chunksize $chunkSize, maxMem ${memPrint(maxMem)} and initialConfig ${memPrint(initialConfig)}")
                }
                return
            }
        }
        def r = reward(rss, mem)
        logInfo("memory alloc'd ${memPrint(mem)}, mem usage ${memPrint(rss)} (${rss*100/mem} %) -> reward $r\n")
        for (i in 0..<numChunks) {
            def oldval = memoryPreferences[i]
            if (i == memIndex ){
                memoryPreferences[i] = oldval + stepSize * (r - memoryAvgReward) * (1 - memoryProbabilities[i])
                logInfo("update rest preferences: memPreferences[$i] = $oldval + $stepSize * ($r - $memoryAvgReward) *  (1 - ${memoryProbabilities[i]}) = ${memoryPreferences[i]}\n")
            } else {
                memoryPreferences[i] = oldval - stepSize * (r - memoryAvgReward) * (memoryProbabilities[i])
                logInfo("update rest preferences: memPreferences[$i] = $oldval - $stepSize * ($r - $memoryAvgReward) *  ${memoryProbabilities[i]} = ${memoryPreferences[i]}\n")
            }
        }

    }

    private void readPrevRewards() {
        logInfo("Searching SQL for Bandit $taskName lastId $lastTaskId and cmd $command")
        def sql = new Sql(TaskDB.getDataSource())
        def searchSql = "SELECT peak_rss,memory,realtime FROM taskrun WHERE task_name = (?) and id > (?) and rl_active = true and wf_name like (?)"
        sql.eachRow(searchSql,[taskName,this.lastTaskId,"%${this.command}%".toString()]) { row ->
            def rss = (long) row.peak_rss
            def mem = (long) row.memory
            logInfo("probabilities BEFORE: $memoryProbabilities")
            updatePreferences(rss,mem)
            updateProbabilities()
            logInfo("probabilities AFTER: $memoryProbabilities")
        }
        sql.close()
        logInfo("Done with SQL for Bandit $taskName")
    }

    double reward(long rss, long memory) {
        double r
        int numChunks = this.numChunks - 2
        if (rss == memory + 1) {
            // if we were killed by the oom killer this is marked in the db by setting rss = mem + 1
            // and we receive -1 * (maxChunk + chunks allocated) as the reward because we have wasted that much memory and must run the task again which will cost more memory
            // for the safeMax and initialConfig states the punishment is capped at -1 or -2 * numChunks
            r = memory < safeMax ? -1*(numChunks + (memory - minMem)/chunkSize) : -2*numChunks
        } else {
            // sometimes rss is > mem but somehow the task succeeded anyways...
            double unusedChunks = Math.max((memory - Math.max(rss,minMem)),0) / ((double) chunkSize)
            r = memory < safeMax ? -1*unusedChunks : -1*numChunks
        }
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

    public synchronized long allocateMem(int failcount, RunType runtype, long previousConfig){
        if(tooShort){
            return initialConfig
        }
        readPrevRewards()
        def index = 0
        long r = 0
        def rand = Math.random()
        double pdf = 0
        for (i in 0..<numChunks-2) {
            pdf += memoryProbabilities[i]
            if (rand <= pdf){
                break
            }
            index++
        }
        if (index < numChunks - 2){
            r = (index + 1)*chunkSize + minMem
        } else if (index == numChunks - 2){
            r = safeMax
        } else if (index == numChunks - 1){
            r = initialConfig
        }
        if(r == 0) {
            logError("$taskName Bandit couldnt pick a memory allocation, are the probabilities okay? ($memoryProbabilities) ... defaulting to original config")
            return initialConfig
        }
        if(runtype == RunType.RETRY && (r <= previousConfig || failcount >= 2)){
            // retry strategy: first try with maxMem, then double that, then try whatever is larger out of 2*2*maxMem or the initial Config, then keep doubling the memory until the task succeeds
            def oldR = r
            if (previousConfig > maxMem || failcount >= 2) {
                r = previousConfig >= initialConfig ? previousConfig * 2 : initialConfig
            } else {
                r = (previousConfig < maxMem && failcount <= 1) ? safeMax : safeMax * 2 // default approach for the first two times we fail
            }
            logError("$taskName Bandit was (probably) killed with ${memPrint(previousConfig)} but the bandit picked ${memPrint(oldR)} as new config (failcount $failcount). Now returning either maxMem or larger: ${memPrint(r)}")
        }
        // the case where the task is killed with the initialConfig is ignored because it should/cant really occur
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
            logError("There was an sql error in checkTooShort(): " + sqlException)
        }
        return tooShort
    }
}
