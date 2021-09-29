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
    double stepSizeCpu
    String taskName
    int numRuns

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

    public GradientBandit(int cpus, String taskName,boolean withLogs){
        this.taskName = taskName
        this.maxCpu = cpus
        this.stepSizeCpu = calculateStepSize()
        this.cpuPreferences = new double[maxCpu] // 0 to start
        this.cpuProbabilities = new double[maxCpu]
        for (i in 0..<maxCpu) {
            cpuProbabilities[i] = 1.0/((double) maxCpu) // initial probability is the same
        }
        this.cpuAvgReward = 0
        this.numRuns = 0
        this.enable_logs = withLogs
    }

    private double calculateStepSize(){
        def stepSize = 0.1
        try{
            // modulate step size to correspond to a reference ratio based on the average realtime for the task
            // step size should be smaller when the rewards are larger so that the bandits dont converge too quickly
            // our reference is the sutton and barto book where for a reward function ranging between 0 and 10 the step size is 0.1
            // reward is influenced the most by realtime and the tasks with realtimes larger than 5k tend to converge too fast
            // a task with avg realtime of 10k should therefore have a step size of 0.01 (assuming 0.1 is the 'normal' step size)
            def sql = new Sql(TaskDB.getDataSource())
            def searchSql = "SELECT AVG(realtime) FROM taskrun WHERE task_name = (?)" // "and rl_active = false"
            sql.eachRow(searchSql,[taskName]) { row ->
                if(row.avg && row.avg > 5000) {
                    //def modFactor = 1000 // we divide by 1000 because the bandit's reward function does too
                    def modFactor = 2000 // 1k was too small for the bandits between 5 and 10k

                    // the commented out stepSize values are reference values that the stepSize should be close to
//                    if(row.avg > 5000) {
////                        stepSize = 0.02
//                    }
//                    if(row.avg > 10000) {
////                        stepSize = 0.01
//                    }

                    // as times get larger the scaling down of the stepsize is too harsh so we nudge it upwards

                    if(row.avg > 20000) {
//                        stepSize = 0.005
                        modFactor = 2000
                    }
                    if(row.avg > 40000) {
//                        stepSize = 0.001
                        modFactor = 5000
                    }
                    // havent seen a task this large yet...
                    if(row.avg > 1000000) {
                        modFactor = 10000
                    }

                    stepSize = modFactor * 0.1 / row.avg
                }
            }
            sql.close()
        } catch (SQLException sqlException) {
            log.info("There was an error: " + sqlException)
        }
        stepSizeCpu = stepSize
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
        def searchSql = "SELECT cpus,cpu_usage,realtime FROM taskrun WHERE task_name = (?) and rl_active = true order by created_at"
        sql.eachRow(searchSql,[taskName]) { row ->
            def cpus = (int) row.cpus
            def usage = (float) row.cpu_usage
            def realtime = (int) row.realtime
            logInfo("Task \"$taskName\": probabilities BEFORE: $cpuProbabilities")
            updateCpuPreferences(cpus,usage,realtime)
            updateCpuProbabilities()
            logInfo("Task \"$taskName\": probabilities AFTER: $cpuProbabilities")
        }
        sql.close()
        logInfo("Done with SQL for Bandit $taskName")
    }

    double reward(int cpuCount, float usage, int realtime) {
        double r = -1 * (realtime/1000)
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
