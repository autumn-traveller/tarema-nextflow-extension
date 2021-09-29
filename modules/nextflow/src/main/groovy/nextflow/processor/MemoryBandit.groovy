package nextflow.processor

import groovy.sql.Sql
import groovy.util.logging.Slf4j
import nextflow.TaskDB

@Slf4j
class MemoryBandit {
    final int numActions = 5
    final int NOOP = 0
    final int DECR_MEM = 1
    final int INCR_MEM = 2
    final int DECR_CPU = 3
    final int INCR_CPU = 4

    int state
    int numStates
    int maxCpu
    long minMem
    long maxMem
    long chunkSize
    double [] [] q
    boolean [] [] visited
    int [] [] timesTried
    float stepSize
    float discount
    String taskName
    float epsilon

    public MemoryBandit(int initialCpu, int maxCpu, long initialMem, long chunkSize, int numChunks, float stepSize, float discount, float epsilon, String taskName){
        // limit our initial state space based on the original config
        if(initialCpu == 1){
            this.maxCpu = maxCpu > 4 ? 4 : maxCpu
        }
        if(initialCpu == 2){
            this.maxCpu = maxCpu > 6 ? 6 : maxCpu
        }
        if(initialCpu > 2){
            this.maxCpu = maxCpu > initialCpu*4 ? initialCpu*4 : maxCpu
        }
        this.maxMem = initialMem + numChunks/2 * chunkSize
        this.minMem = initialMem - numChunks/2 * chunkSize

        //TODO whats the plan for numStates and chunkSize
        this.numStates = numChunks
        this.chunkSize = chunkSize

        this.q = new double[numChunks][]
        for (i in 0..<numChunks) {
            q[i] = new double[numActions]
            for (j in 0..<numActions){
                q[i][j] = 0
            }
        }
        this.visited = new boolean[numChunks][]
        for (i in 0..<numChunks) {
            visited[i] = new boolean[numActions]
            for (j in 0..<numActions){
                visited[i][j] = false
            }
        }
        this.timesTried = new int[numChunks][]
        for (i in 0..<numChunks) {
            timesTried[i] = new int[numActions]
            for (j in 0..<numActions){
                timesTried[i][j] = 0
            }
        }
        this.stepSize = stepSize
        this.discount = discount
        this.epsilon = epsilon
        this.taskName = taskName
    }


//    private void readPrevRewards() {
//        // TODO
//        log.info("Searching SQL for Bandit $taskName")
//        def sql = new Sql(TaskDB.getDataSource())
//        def searchSql = "SELECT cpus,cpu_usage, FROM taskrun WHERE task_name = (?)"
//        sql.eachRow(searchSql,[taskName]) { row ->
//            def cpus = (int) row.cpus
//            def usage = (float) row.cpu_usage
//            def realtime = (int) row.realtime
//            log.info("Task \"$taskName\": prefs and probabilities BEFORE: $cpuPreferences , $cpuProbabilities")
//            updatePreferences(cpus,usage,realtime)
//            updateProbabilities()
//            log.info("Task \"$taskName\": prefs and probabilities AFTER: $cpuPreferences , $cpuProbabilities")
//        }
//        sql.close()
//        log.info("Done with SQL for Bandit $taskName")
//    }

    private void readPrevRewards() {
        // TODO
        log.info("Searching SQL for Bandit $taskName")
        def sql = new Sql(TaskDB.getDataSource())
        def searchSql = "SELECT vmem, peak_vmem, rss, peak_rss, memory FROM taskrun WHERE task_name = (?) order by created_at"
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

    int reward(int cpu, float cpu_usage, long mem_usage, long rss, long peak_rss, long memAllocd) {
        //TODO
    }

//    private int pickCpu(double rand){
//        int ret = 0
//        double pdf = 0
//        for (i in 0..<maxCpu) {
//            pdf += cpuProbabilities[i]
//            if (rand <= pdf){
//                return i*chunkSize + 1
//            }
//        }
//        log.error("$taskName Bandit couldnt pick a cpu, are the probabilities okay? ($cpuProbabilities) ... defaulting to 1 cpu")
//        return -1
//    }

    void logBandit(){

    }

    public int allocateMem(){
        //TODO
    }

}
