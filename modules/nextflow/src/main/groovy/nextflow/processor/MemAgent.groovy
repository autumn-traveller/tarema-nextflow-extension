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
    int maxStateMem
    long minMem
    long maxMem
    long chunkSize
    Map<Long,StateActionData> states = [:]
    float stepSize
    float discount
    String taskName
    String command
    String runName
    float epsilon
    int lastTaskId
    int count
    boolean tooShort = false
    boolean withLogs

    private void logInfo(String var1, Object... var2){
        if(withLogs){
            log.info("Agent \"$taskName\" ($count): $var1",var2)
        }
    }

    private void logError(String var1, Object... var2){
        def f = new File("errlog")
        f.append("WF $command, Run name $runName, Agent \"$taskName\" ($count): $var1\n")
        log.error("Agent \"$taskName\" ($count): $var1",var2)
    }

    private static long toMega(m) { m >> 20 }

    private static String memStr(long m){
        return "${toMega(m)} MB"
    }

    public MemAgent(long minMem, long maxMem, int numChunks, String taskName, String command, String runName, boolean withLogs){
        this.withLogs = withLogs
        this.taskName = taskName
        this.count = 0
        this.command = command.split("\\./evaluation_assets/nf-core/")[1].replace('/','')
        this.runName = runName
        if(checkTooShort()){
            return
        }

        this.minMem = minMem
        this.maxMem = maxMem
        this.chunkSize = (long) (maxMem - minMem / numChunks)
        this.maxStateMem = numChunks

        logInfo("agent config: withLogs $withLogs, command $command, Memory chunksize : ${memStr(chunkSize)}, min: $minMem (${memStr(minMem)}) and max $maxMem (${memStr(maxMem)})}")

        String str = ""
        for (m in 0..<maxStateMem){
            long mem = minMem + m*chunkSize
            str += "Mem option $m: ${memStr(mem)} "
            def d = new StateActionData(numActions)
            states.put(mem,d)
        }
        logInfo(str)
        logInfo('ALL STATES',states)

        this.state = (long) (minMem + chunkSize*(numChunks/2))
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

    private void readPrevRewards() {
        logInfo("Starting SQL for Agent")
        def sql = new Sql(TaskDB.getDataSource())
        def searchSql = "SELECT taskrun.id,realtime,memory,peak_rss,taskrun.duration FROM taskrun LEFT JOIN runs ON taskrun.run_name = runs.run_name WHERE task_name = (?) and taskrun.rl_active = true and taskrun.id > (?) and (command like (?) or taskrun.run_name = (?)) ORDER BY created_at asc"
        sql.eachRow(searchSql,[taskName, lastTaskId, "%${this.command}%".toString(), runName]) { row ->
            this.lastTaskId = (int) row.id
            def realtime = (long) row.realtime
            def rss =  (long) row.peak_rss
            def mem = (long) row.memory
            double mem_usage = ((double) rss)/((double) mem)
            def actionTaken = (int) row.duration // dirty hack to store actions for now
            def r = reward(realtime,mem_usage,mem,rss)

            logInfo("time ${realtime/1000} seconds, avg time is ${avgRealtime/1000} seconds (${1-realtime/avgRealtime} speedup), mem ${memStr(mem)}, rss ${memStr(rss)} (mem_usage ${mem_usage*100}%) -> reward $r")

            def newState = mem
            def newData = states.get(newState)
            if(!newData){
                logError("Unable to find data for state $newState in states map: prevState = $state")
                throw new Error("Undefined state")
            }

            long prevState

            switch(actionTaken){
                case NOOP:
                    prevState = mem
                    break
                case INCR_MEM:
                    prevState = mem-chunkSize
                    break
                case DECR_MEM:
                    prevState = mem+chunkSize
                    break
                default:
                    logError("Bad action in DB $actionTaken")
                    throw new Error("Undefined state")
                    break
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

            if(++count > 25){
                epsilon = 0.25
            } else if(count > 50){
                epsilon = 0.1
            } else if (count > 75){
                epsilon = 0.02
            }

        }
        sql.close()
        logInfo("Done with SQL for Agent")
    }

    float reward(long realtime, double mem_usage, long memory, long rss) {
        double unusedChunks = ((double) Math.max((memory - rss),0)) / ((double) chunkSize)
        return (rss != memory+1) ? -1*unusedChunks : -2*memory/chunkSize // if we were killed by the oom killer we receive -2 * the memory allocated
    }

    void logBandit(){

    }

    boolean actionAllowed(int a){
        if(a == DECR_MEM && state == minMem){
            curData.q[a] = Double.NEGATIVE_INFINITY
            return false
        }
        if(a == INCR_MEM && state == maxMem){
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
            logInfo("Picking random action: $action")
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

    public synchronized boolean takeAction(TaskConfig config){
        if(tooShort) {
            return false
        }
        readPrevRewards()
        def action = this.takeNewAction()
        long mem = this.state
        logInfo("Proposed action is $action for state $state and cpuInd $currentCpuInd")
        switch(action){
            case NOOP:
                break
            case DECR_MEM:
                mem -= chunkSize
                break
            case INCR_MEM:
                mem += chunkSize
                break
            default:
                logError("invalid action picked: $action in state $state")
                return false
        }
        if(mem < minMem || mem > maxMem ){
            logError("Invalid value chosen for mem: ${memStr(mem)} (chunk size is ${memStr(chunkSize)}) and min is ${memStr(minMem)} and max is ${memStr(maxMem)} action was/is $action in state $state")
            return false
        }
        config.put('memory', new MemoryUnit(mem))
        config.put('action',action)
        logInfo("New config: ${memStr(mem)} mem")
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
                   avgRealtime = row.avg
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
