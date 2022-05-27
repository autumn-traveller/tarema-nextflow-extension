package nextflow.processor

import java.sql.SQLException
import groovy.sql.Sql
import groovy.util.logging.Slf4j
import nextflow.TaskDB

@Slf4j
class GradientBandit {
    int numOptions
    int[] cpuOptions
    double[] cpuPreferences
    double[] cpuProbabilities
    double cpuAvgReward
    double stepSizeCpu
    String taskName
    int numRuns
    int lastTaskId
    boolean tooShort = false

    public GradientBandit(int cpus, String taskName, boolean withLogs){
        this.taskName = taskName
        this.stepSizeCpu = calculateStepSize()
        if (tooShort) {
            return
        }
        if (taskName in ['FASTQC','MERGED_LIB_PRESEQ','BAMBU'] ) {
            this.cpuOptions = [4,6,8,10,12,14,16]
        } else {
            this.cpuOptions = [1,2,4,6,8,10,12,14]
        }
        this.numOptions = cpuOptions.size()
        this.cpuPreferences = new double[numOptions] // 0 to start
        this.cpuProbabilities = new double[numOptions]
        for (i in 0..<numOptions) {
            cpuProbabilities[i] = 1.0/((double) numOptions) // initial probability is the same
        }
        this.cpuAvgReward = 0
        this.numRuns = 0
        this.lastTaskId = 0
        this.enable_logs = withLogs
        this.readPrevRewards()
    }

//    private void initCpuOptions(int configCpus) {
//        // search sql for vanilla data on the task- pick the cpuOptions based on either this or the initial config if there is no data available
//        int base = -1
//        def sql = new Sql(TaskDB.getDataSource())
//        def searchSql = "SELECT COUNT(cpu_usage), AVG(cpu_usage)/100 FROM taskrun WHERE task_name = (?) and rl_active = false"
//        try {
//            sql.firstRow(searchSql,[taskName]) { row ->
//                if (row && row.count >= 5) {
//                    base = Math.round(row.cpu_usage as float)
//                }
//            }
//            sql.close()
//        } catch (SQLException sqlException) {
//            log.info("There was an error: " + sqlException)
//        }
//        populateOptions(base,configCpus)
//    }

//    private void populateOptions(int base, int configCpus) {
//        // 3 different sets of options: low use, normal, high use
//        // if we have existing data we pick one of those three, if there is no existing data on the task we pick low or normal
//
//        int[] low = [1,2,3,4,5,6,7,8]
//        int[] med = [1,2,4,6,8,10,12,14]
//        int[] high = [4,6,8,10,12,14,16]
//
//        if (base == -1){
//            this.cpuOptions = configCpus > 2 ? med : low
//            this.numOptions = cpuOptions.size()
//            return
//        }
//
//        def usage = base / configCpus
//        if (usage >= 0.75 && configCpus >= 4) {
//            this.cpuOptions = high
//            this.numOptions = cpuOptions.size()
//            return
//        } else if base <
//
//    }

    private double calculateStepSize(){
        def stepSize = 0.1
        try{
            // modulate step size to correspond to a reference ratio based on the average realtime for the task
            // step size should be smaller when the rewards are larger so that the bandits dont converge too quickly
            // our reference is the sutton and barto book where for a reward function ranging between 0 and 10 the step size is 0.1
            // reward is influenced the most by realtime and the tasks with realtimes larger than 5k tend to converge too fast
            // a task with avg realtime of 10k should therefore have a step size of 0.01 (assuming 0.1 is the 'normal' step size)
            def sql = new Sql(TaskDB.getDataSource())
            def searchSql = "SELECT COUNT(realtime), AVG(realtime) FROM taskrun WHERE task_name = (?)" // "and rl_active = false"
            sql.eachRow(searchSql,[taskName]) { row ->
                if(row.avg && row.avg as int > 5000) {
                    //def modFactor = 1000 // we divide by 1000 because the bandit's reward function does too
                    def modFactor = 2000 // 1k was too small for the bandits between 5 and 10k

                    // the commented out stepSize values are reference values that the stepSize should be close to
//                    if(row.avg > 5000) {
////                        stepSize = 0.02
//                    }
//                    if(row.avg > 10000) {
////                        stepSize = 0.01
//                    }

                    // as times get larger the scaling down of the stepsize can be too harsh so we nudge it a upwards

                    if(row.avg > 20000) {
//                        stepSize = 0.005
                        modFactor = 2000
                    }
                    if(row.avg > 40000) {
//                        stepSize = 0.001
                        modFactor = 2500
                    }
                    // havent seen a task this large yet...
                    if(row.avg > 1000000) {
                        modFactor = 5000
                    }

                    stepSize = modFactor * 0.1 / row.avg
                } else if (row.count && row.count as int > 5 && row.avg && row.avg as int < 1000){
                    tooShort = true // these tasks are too short for the nextflow metrics to be accurate so we ignore them
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
        for (i in 0..<numOptions) {
            s += Math.exp(cpuPreferences[i])
        }
        for (i in 0..<numOptions) {
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
        for (i in 0 ..< numOptions) {
            def oldval = cpuPreferences[i]
            if (cpuOptions[i] == cpus){
                cpuPreferences[i] = cpuPreferences[i] + stepSizeCpu * (r - cpuAvgReward) * (1 - cpuProbabilities[i])
                logInfo("Task \"$taskName\": update (allocd cpus) preference: cpuPreferences[$i] = $oldval + $stepSizeCpu * ($r - $cpuAvgReward) * (1 - ${cpuProbabilities[i]}) = ${cpuPreferences[i]}\n")
            } else {
                cpuPreferences[i] = cpuPreferences[i] - stepSizeCpu * (r - cpuAvgReward) * (cpuProbabilities[i])
                logInfo("Task \"$taskName\": update rest preferences: cpuPreferences[$i] = $oldval - $stepSizeCpu * ($r - $cpuAvgReward) *  ${cpuProbabilities[i]} = ${cpuPreferences[i]}\n")

            }
        }

    }

    private void readPrevRewards() {
        logInfo("Searching SQL for Bandit $taskName")
        def sql = new Sql(TaskDB.getDataSource())
        def searchSql = "SELECT id,cpus,cpu_usage,realtime FROM taskrun WHERE task_name = (?) and rl_active = true and id > (?) order by created_at asc"
        sql.eachRow(searchSql,[taskName,lastTaskId]) { row ->
            if(row.cpu_usage != null && row.cpus != null && (row.cpus as int) in this.cpuOptions){
                this.lastTaskId = row.id as int
                def cpus = row.cpus as int
                def usage = row.cpu_usage as float
                def realtime = row.realtime as int
                logInfo("Task \"$taskName\": probabilities BEFORE: $cpuProbabilities")
                updateCpuPreferences(cpus,usage,realtime)
                updateCpuProbabilities()
                logInfo("Task \"$taskName\": probabilities AFTER: $cpuProbabilities")
            }
        }
        sql.close()
        logInfo("Done with SQL for Bandit $taskName")
    }

    double reward(int cpuCount, float usage, int realtime) {
        // aim for 100% usage of the allocated cpus
        // highest reward comes with 100% usage of the available cpus and minimal runtime
        // sometimes usage > 100% * numcpus will be reported so we take the smaller value of 100% and the actual usage
        // reward is negative so we want to keep its absolute value small since we are using it with the exp() function
        // divide by 1000 because realtime measurements are inaccurate for tasks with runtimes smaller than 1s
        double r = -1 * (realtime/1000) * (1 + cpuCount - Math.min(usage,cpuCount*100)/100)
//        double r = -1 * cpuCount * realtime
        cpuAvgReward = (numRuns * cpuAvgReward + r)/(numRuns + 1)
        numRuns++
        return r
    }

    private int pickCpu(double rand){
        double pdf = 0
        for (i in 0..<numOptions) {
            pdf += cpuProbabilities[i]
            if (rand <= pdf){
                return cpuOptions[i]
            }
        }
        log.error("$taskName Bandit couldnt pick a cpu, are the probabilities okay? ($cpuProbabilities) ... defaulting to configured cpus")
        return -1
    }

    void logBandit(){
        def s = ""
        s += "Bandit $taskName\n"
        for (i in 0..<numOptions) {
            s += "Action ${i+1} cpus: Preference ${cpuPreferences[i]} Probability ${cpuProbabilities[i]}\n"
        }
        s += "$cpuAvgReward"
        logInfo(s)
    }

    public synchronized int allocateCpu(){
        if(tooShort){
            return -1
        }
        readPrevRewards()
        //logBandit()
        return pickCpu(Math.random())
    }

}
