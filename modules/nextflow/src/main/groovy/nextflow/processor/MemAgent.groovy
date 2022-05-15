package nextflow.processor

import groovy.sql.Sql
import groovy.util.logging.Slf4j
import nextflow.TaskDB
import nextflow.util.MemoryUnit

import java.sql.SQLException

//class State {
//    long m
//    int c
//    private String h
//    State(long m, int c){
//        this.m = m
//        this.c = c
//        this.h = "$m$c"
//    }
//
//    private static long toMega(m) { m >> 20 }
//
//    private static String memPrint(long m){
//        return "${toMega(m)} MB"
//    }
//
//    @Override
//    String toString() {
//        return "(mem ${memPrint(m)} , cpu $c)"
//    }
//
//    @Override
//    int hashCode() {
//        return h.hashCode()
//    }
//
//    @Override
//    boolean equals(Object obj) {
//        return (obj != null) && (obj instanceof State) && (obj.c == this.c && obj.m == this.m)
//    }
//}

//class StateActionData {
//    double [] q
//    boolean [] visited
//    int [] timesTried
//
//    StateActionData(int numActions){
//        q = new double[numActions]
//        visited = new boolean[numActions]
//        timesTried = new int[numActions]
//    }
//
//    @Override
//    String toString() {
//        return "(q $q , tried $timesTried)"
//    }
//}

@Slf4j
class MemAgent {
    final int numActions = 3
    final int NOOP = 0
    final int DECR_MEM = 1
    final int INCR_MEM = 2

    long state
    StateActionData curData
    long maxMem
    long safeMax
    long minMem
    long chunkSize
    long initialConfig
    int numChunks
    int maxStateMem
    Map<Long,StateActionData> states = [:]
    float stepSize
    float discount
    float epsilon
    String taskName
    String command
    String runName
    int count
    boolean withLogs
    boolean tooShort = false
    int lastTaskId

    private static long toMega(m) { m >> 20 }
    private static long toGiga(m) { m >> 30 }

    private static String memPrint(long m){
        return "${toMega(m)} MB"
    }

    private void logInfo(String var1, Object... var2){
        if(withLogs){
            log.info("MemAgent \"$taskName\" ($count): $var1",var2)
        }
    }

    private void logError(String var1, Object... var2){
        def f = new File("errlog")
        f.append("WF $command, Run name $runName, Agent \"$taskName\" ($count): $var1\n")
        log.error("Agent \"$taskName\" ($count): $var1",var2)
    }

    public MemAgent(long initialConfig, int numChunks, String taskName, String command, String runName, boolean withLogs){
        this.taskName = taskName
        this.initialConfig = initialConfig
        this.command = command.split("\\./evaluation_assets/nf-core/")[1].replace('/','')
        this.withLogs = withLogs
        this.count = 0
        this.runName = runName
        this.numChunks = numChunks
        if(checkTooShort()){
            return
        }
        // now we pick the states based on historic usage
        // however we add two extra states: safeMax and the initialConfig
        // usually the initialConfig is gigantic so it is "always safe" against the oom killer
        // safeMax is double the maxMem and is usually pretty decent insurance while not being as wasteful as the initial config
        // we use a slightly more aggressive reallocation strategy when the bandit dies: we retry with safeMax and if that fails to then we retry with the initial config
        this.minMem = 7 << 20
        if (!pollHistoricUsage()){
            // default values in case of an error
            this.maxMem = initialConfig
            this.safeMax = initialConfig
            this.minMem = 7 << 20
        }
        if (taskName in ['qualimap','damageprofiler','adapter_removal'] && minMem < (1 << 30)) { // add other tasks which use java and require at least 1GB of heap space as needed
            logInfo("task $taskName is in the list of java task with a 1GB min. heap space requirement")
            minMem = 1 << 30
            maxMem += (1 << 30)
            safeMax = 2*maxMem
        }
        this.chunkSize = (long) ((maxMem - minMem) / numChunks)
        this.maxStateMem = numChunks

        logInfo("agent config: withLogs $withLogs, command $command, Memory chunksize : ${memPrint(chunkSize)}, min: $minMem (${memPrint(minMem)}), max: $maxMem (${memPrint(maxMem)})}, safeMax: ${memPrint(safeMax)}, initialConfig: ${memPrint(safeMax)}")

        String str = ""
        for (m in 0..<maxStateMem){
            long mem = minMem + m*chunkSize
            str += "Mem option $m: ${memPrint(mem)} "
            def d = new StateActionData(numActions)
            states.put(mem,d)
        }
        str += "Mem option safeMax: ${memPrint(safeMax)} "
        def d = new StateActionData(numActions)
        states.put(safeMax,d)
        str += "Mem option initialConfig: ${memPrint(initialConfig)} "
        d = new StateActionData(numActions)
        states.put(initialConfig,d)
        logInfo(str)
        logInfo('ALL STATES',states)

        this.state = (long) (minMem + chunkSize*((int)numChunks/2))
        this.curData = states.get(this.state)
        if(!this.curData){
            logError("Unable to find initial state in states map! ($minMem mem, statesmap = $states )")
            throw new Error("Undefined state")
        }

        //TODO what should most of these be?
        this.discount = 1
        this.stepSize = 0.1
        this.epsilon = 0.5 // decrease epsilon over time

        this.lastTaskId = 0
        readPrevRewards()
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
                    logInfo("historic data makes no sense, max ${memPrint(max)} is less than min ${memPrint(min)}")
                    return false
                }
                this.minMem = avg - Math.abs(stddev) > this.minMem ? avg - Math.abs(stddev) : this.minMem
                if (max <= this.minMem) {
                    logInfo("max is less than the adjusted minimum ${memPrint(this.minMem)}")
                    this.maxMem = this.minMem * 2
                } else {
                    this.maxMem = max * 2
                }
                this.safeMax = this.maxMem * 2
            }
            sql.close()
        } catch (SQLException sqlException) {
            logError("There was an sql error when polling historic data: " + sqlException)
            return false
        }
        return true
    }

    private void readPrevRewards() {
        logInfo("Starting SQL for Agent")
        def sql = new Sql(TaskDB.getDataSource())
        def searchSql = "SELECT taskrun.id,realtime,memory,peak_rss,taskrun.duration,rchar FROM taskrun LEFT JOIN runs ON taskrun.run_name = runs.run_name WHERE task_name = (?) and taskrun.rl_active = true and taskrun.id > (?) and (command like (?) or taskrun.run_name = (?)) ORDER BY created_at asc"
        sql.eachRow(searchSql,[taskName, lastTaskId, "%${this.command}%".toString(), runName]) { row ->
            if (row.memory && row.duration && row.rchar) {
                this.lastTaskId = (int) row.id
                long realtime = row.realtime ? row.realtime : 0
                long rss = row.peak_rss ? row.peak_rss : 0
                def mem = (long) row.memory
                double mem_usage = ((double) rss) / ((double) mem)
                int actionTaken = (int) row.duration // dirty hack to store the previous state
                long prevState = (long) row.rchar // dirty hack part2
                // we cant add new columns to the db because we need to populate it with older db dumps of vanilla data so we reuse columns we dont care about for other purposes
                def r = reward(mem, rss)

                def newState = mem
                def newData = states.get(newState)
                if (!newData) {
                    logError("Unable to find data for state $newState in states map: prevState = $state")
                    throw new Error("Undefined state")
                }

                logInfo("Prev Action was $actionTaken, we were in $prevState and are now in $newState")

                def prevData = states.get(prevState)

                def qOld = prevData.q[actionTaken]

                double newQMax = Collections.max(newData.q as Collection<? extends Double>)
                logInfo("qMax for ${newState} is $newQMax over ${newData.q}")
                prevData.q[actionTaken] = qOld + stepSize * (r + discount * newQMax - qOld)
                logInfo("Old q for ${prevState} = $qOld , New q = $qOld + $stepSize * ($r + $discount * $newQMax - $qOld) = ${prevData.q[actionTaken]}")

                prevData.timesTried[actionTaken]++
                prevData.visited[actionTaken] = true

                this.state = newState
                this.curData = newData

                if (++count > 25) {
                    epsilon = 0.25
                } else if (count > 50) {
                    epsilon = 0.1
                } else if (count > 75) {
                    epsilon = 0.02
                }
            }

        }
        sql.close()
        logInfo("Done with SQL for Agent")
    }

    float reward(long memory, long rss) {
        double unusedChunks = ((double) Math.max((memory - rss),0)) / ((double) chunkSize)
        return (rss != memory+1) ? -1*unusedChunks : -2*memory/chunkSize // if we were killed by the oom killer we receive -2 * the memory allocated since we wasted it once and must rerun it with at least as much again
    }

    void logBandit(){

    }

    boolean actionAllowed(int a){
        if(a == DECR_MEM && state == minMem){
            curData.q[a] = Double.NEGATIVE_INFINITY
            return false
        }
        if(a == INCR_MEM && state == initialConfig){
            curData.q[a] = Double.NEGATIVE_INFINITY
            return false
        }
        return a >= 0 && a < numActions
    }

    private int takeNewAction(){
        if(Math.random() < epsilon) {
            // pick a random action rather than the maximizing one
            int action
            do {
                action = (int) Math.floor(Math.random() * numActions)
            } while(!actionAllowed(action))
            return action
        }

        double x = Double.NEGATIVE_INFINITY
        int aMax = 0
        for (a in 0..<numActions){
            if(!actionAllowed(a)){
                continue
            }
            if(!curData.visited[a]){
                return a
            }
            if(curData.q[a] > x){
                x = curData.q[a]
                aMax = a
            }
        }
        logInfo("Picked aMax $aMax from ${curData.q}")
        return aMax
    }

    public synchronized boolean takeAction(TaskConfig config, int failcount, TaskProcessor.RunType runtype, long previousConfig){
        if(tooShort) {
            return false
        }
        readPrevRewards()
        def action = this.takeNewAction()
        long mem = this.state
        logInfo("Proposed action is $action for state ${memPrint(state)}")
        switch(action){
            case NOOP:
                break
            case DECR_MEM:
                if (mem == safeMax) {
                    mem = maxMem
                } else if (mem == initialConfig) {
                    mem = safeMax
                } else {
                    mem -= chunkSize
                }
                break
            case INCR_MEM:
                if (mem == safeMax) {
                    mem = initialConfig
                } else if (mem == maxMem) {
                    mem = safeMax
                } else {
                    mem += chunkSize
                }
                break
            default:
                logError("invalid action picked: $action in state ${memPrint(state)}")
                return false
        }
        if(mem < minMem || (mem > maxMem && (mem != safeMax || mem != initialConfig))){
            logError("Invalid value chosen for mem: ${memPrint(mem)} (chunk size is ${memPrint(chunkSize)}) and min: $minMem (${memPrint(minMem)}), max: $maxMem (${memPrint(maxMem)})}, safeMax: ${memPrint(safeMax)}, initialConfig: ${memPrint(safeMax)}")
            return false
        }

        if(runtype == TaskProcessor.RunType.RETRY && (mem <= previousConfig || failcount >= 2)){
            // retry strategy: first try with safeMax then double that, then retry with whatever is larger out of 2*safeMax or the initial Config, then keep doubling the memory until the task succeeds
            def old = mem
            if (previousConfig > safeMax || failcount > 2) {
                mem = Math.min(previousConfig >= initialConfig ? previousConfig * 2 : initialConfig, 120 << 30) // 120 GB is basically the machine's max
            } else {
                mem = (previousConfig < safeMax && failcount <= 1) ? safeMax : safeMax * 2 // default approach for the first two times we fail
            }
            logError("$taskName Bandit was (probably) killed with ${memPrint(previousConfig)} but the bandit picked ${memPrint(old)} as new config (failcount $failcount). Now returning either maxMem or larger: ${memPrint(mem)}")
        }

        config.put('memory', new MemoryUnit(mem))
        config.put('memAction',action) // insert into duration column later
        // even if mem has been artificially changed because of repeated failures we keep $action and $state the same
        // from the perspective of the agent is has "tunneled" into a new (higher) memory state because its action was a bad choice
        config.put('prevMemState',state) // insert into rchar column later
        logInfo("New config: ${memPrint(mem)} mem")
        this.curData.visited[action] = true
        this.state = mem
        return true
    }

    boolean checkTooShort(){
        tooShort = true
        try{
            def sql = new Sql(TaskDB.getDataSource())
            def searchSql = "SELECT COUNT(realtime), AVG(realtime) FROM taskrun WHERE task_name = (?)" // "and rl_active = false"
            sql.eachRow(searchSql,[taskName]) { row ->
               if (row.count && row.count as int >= 5 && row.avg && row.avg as int > 1000){
                   tooShort = false // these tasks are long enough for the nextflow metrics to be accurate
               }
            }
            sql.close()
        } catch (SQLException sqlException) {
            log.error("There was an error: " + sqlException)
        }
        return tooShort
    }
}
