package nextflow.processor

import groovy.sql.Sql
import groovy.util.logging.Slf4j
import nextflow.TaskDB
import nextflow.util.MemoryUnit

import java.sql.SQLException

class State {
    long m
    int c
    private String h
    State(long m, int c){
        this.m = m
        this.c = c
        this.h = "$m$c"
    }

    private static long toMega(m) { m >> 20 }

    private static String memPrint(long m){
        return "${toMega(m)} MB"
    }

    @Override
    String toString() {
        return "(mem ${memPrint(m)} , cpu $c)"
    }

    @Override
    int hashCode() {
        return h.hashCode()
    }

    @Override
    boolean equals(Object obj) {
        return (obj != null) && (obj instanceof State) && (obj.c == this.c && obj.m == this.m)
    }
}

class StateActionData {
    double [] q
    boolean [] visited
    int [] timesTried

    StateActionData(int numActions){
        q = new double[numActions]
        visited = new boolean[numActions]
        timesTried = new int[numActions]
    }

    @Override
    String toString() {
        return "(q $q , tried $timesTried)"
    }
}

@Slf4j
class QAgent {
    final int numActions = 5
    final int NOOP = 0
    final int DECR_MEM = 1
    final int INCR_MEM = 2
    final int DECR_CPU = 3
    final int INCR_CPU = 4

    int prevAction
    int action
    State prevState
    State state
    StateActionData curData
    int maxStateMem
    int maxStateCpus
    int minCpu
    int currentCpuInd
    int maxCpu
    int [] cpuOptions
    long minMem
    long maxMem
    long chunkSize
    Map<State,StateActionData> states = [:]
    float stepSize
    float discount
    String taskName
    float epsilon
    int lastTaskId
    boolean tooShort = false
    boolean withLogs
    int count

    private void logInfo(String var1, Object... var2){
        if(withLogs){
            log.info("Agent \"$taskName\" ($count): $var1",var2)
        }
    }

    private static long toMega(m) { m >> 20 }

    private static String memStr(long m){
        return "${toMega(m)} MB"
    }

    public QAgent(int initialCpu, int maxCpu, long initialMem, String taskName, boolean withLogs){
        this.withLogs = withLogs
        this.taskName = taskName
        this.count = 0
        if(checkTooShort()){
            return
        }
//        this.chunkSize = initialMem >> 2 // 5 possible memory states : -1/2 -1/4 0 +1/4 +1/2
        this.chunkSize = initialMem >> 1 // 3 possible memory states : -1/2 0 +1/2
//        this.maxStateMem = 5
        this.maxStateMem = 3
//        this.maxMem = initialMem + 2 * chunkSize
        this.maxMem = initialMem + chunkSize
//        this.minMem = initialMem - 2 * chunkSize
        this.minMem = initialMem - chunkSize


        logInfo("Memory chunksize : ${memStr(chunkSize)} min: $minMem (${memStr(minMem)}) and max $maxMem (${memStr(maxMem)})}")

        // limit our initial state space based on the original config
        if(initialCpu == 1){
            this.minCpu = 1
            this.maxCpu = 4
            this.cpuOptions = [1, 2, 4]
            this.currentCpuInd = 0
        }
        if(initialCpu == 2){
            this.minCpu = 1
            this.maxCpu = 6
            this.cpuOptions = [1, 2, 4, 6]
            this.currentCpuInd = 1
        }
        if(initialCpu > 2 && initialCpu < 7){
            this.minCpu = 2
            this.maxCpu = 8
            this.cpuOptions = [2, 4, 6, 8]
            this.currentCpuInd = 2
        }

        if(initialCpu > 7){
            this.minCpu = 6
            this.maxCpu = 14
            this.cpuOptions = [6, 10, 14]
            this.currentCpuInd = 1
        }

        this.maxStateCpus = this.cpuOptions.size()
        String str = ""
        for (m in 0..<maxStateMem){
            str += "Mem option $m: ${memStr(initialMem + (m-2)*chunkSize)} "
            for (int c : this.cpuOptions){
                def s = new State((long)(initialMem + (m-2)*chunkSize),c)
                def d = new StateActionData(numActions)
                states.put(s,d)
            }
        }
        logInfo(str)
        logInfo('ALL STATES',states)

        this.prevState = null
        this.prevAction = -1
        this.state = new State(initialMem,this.cpuOptions[currentCpuInd])
        this.curData = states.get(this.state)
        if(!this.curData){
            log.error("Unable to find initial state in states map! (${this.cpuOptions[this.currentCpuInd]} cpus, $initialMem mem, statesmap = $states )")
            throw new Error("Undefined state")
        }

        //TODO what should most of these be?
        this.discount = 1
        this.stepSize = 0.25
        this.epsilon = 0.5 // decrease epsilon over time

        this.lastTaskId = 0
        readPrevRewards()
    }

    private void readPrevRewards() {
        logInfo("Starting SQL for Agent")
        def sql = new Sql(TaskDB.getDataSource())
        def searchSql = "SELECT id,cpus,cpu_usage,realtime,memory,mem_usage FROM taskrun WHERE task_name = (?) and rl_active = true and id > (?) order by created_at asc"
        sql.eachRow(searchSql,[taskName,lastTaskId]) { row ->
            this.lastTaskId = (int) row.id
            def cpus = (int) row.cpus
            def cpu_usage = (float) row.cpu_usage
            def realtime = (long) row.realtime
            def mem_usage = (float) row.mem_usage
            def mem = (long) row.memory
            def r = reward(cpus,cpu_usage,realtime,mem_usage,mem)

            logInfo("cpus $cpus usage $cpu_usage mem ${memStr(mem)} mem_usage $mem_usage -> reward $r")

            if(cpus != this.state.c){
                prevAction = cpus > this.state.c ? INCR_CPU : DECR_CPU
                logInfo("Prev Action was $prevAction, we were in ${this.state}")
            } else if (mem != this.state.m){
                prevAction = mem > this.state.m ? INCR_MEM : DECR_MEM
                logInfo("Prev Action was $prevAction, we were in ${this.state}")
            } else {
                logInfo("Prev Action was NOOP, we were in ${this.state}")
                prevAction = NOOP
            }

            def newState = new State(mem,cpus)
            def newData = states.get(newState)
            logInfo("Now we are in ${newState}")
            if(!newData){
                log.error("Unable to find state (c: $cpus m: ${memStr(mem)} in states map")
                throw new Error("Undefined state")
            }
            def qOld = curData.q[prevAction]
            double newQMax = Collections.max(newData.q as Collection<? extends Double>)
//            log.warn("maxdafuq: ${newData.q} , ${newData.q.max()} ")
            curData.q[prevAction] = qOld + stepSize * (r + discount * newQMax - qOld)
            logInfo("Old q = $qOld , New q = $qOld + $stepSize * ($r + $discount * $newQMax - $qOld = ${curData.q[prevAction]}")

            curData.timesTried[prevAction]++
            curData.visited[prevAction] = true

            this.state = newState
            this.curData = newData

            if(++count > 25){
                epsilon = 0.25
            } else if(count > 75){
                epsilon = 0.1
            } else if (count > 150){
                epsilon = 0.02
            }

        }
        sql.close()
        logInfo("Done with SQL for Agent")
    }

    float reward(int cpus, float cpu_usage, long realtime, float mem_usage, long memAllocd) {
        //TODO: try another function?
//        return cpus*5 + Math.min(90f,cpu_usage/cpus) + Math.min(65f,mem_usage) // maximized when resource usage is high
        return cpus*5 + Math.min(90f,cpu_usage/cpus) + Math.min(65f,10*mem_usage) // increase mem reward for testing with low usage workflows
    }

    void logBandit(){

    }

    boolean actionAllowed(int a){
        if(a == DECR_CPU && state.c == minCpu){
            return false
        }
        if(a == INCR_CPU && state.c == maxCpu){
            return false
        }
        if(a == DECR_MEM && state.c == minMem){
            return false
        }
        if(a == INCR_MEM && state.c == maxMem){
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

        double x = 0
        int aMax = 0
        for (a in 0..<numActions){
            if(!actionAllowed(a)){
                continue
            }
            if(!curData.visited[a]){
                curData.visited[a] = true
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
        //TODO if too short...
        if(tooShort) {
            return false
        }
        readPrevRewards()
        action = this.takeNewAction()
        long mem = this.state.m
        switch(action){
            case NOOP:
                break
            case DECR_MEM:
                mem -= chunkSize
                break
            case INCR_MEM:
                mem += chunkSize
                break
            case DECR_CPU:
                this.currentCpuInd -= 1
                break
            case INCR_CPU:
                this.currentCpuInd += 1
                break
            default:
                log.error("invalid action picked: $action")
                return false
        }
        int cpus = this.cpuOptions[this.currentCpuInd]
        if(mem < minMem || mem > maxMem ){
            log.error("Invalid value for mem: ${memStr(mem)} (chunk size is ${memStr(chunkSize)}) and minMem is ${memStr(minMem)}")
            return false
        }
        if(cpus <= 0 || cpus > maxCpu){
            log.error("Invalid value for cpus: $cpus (max cpus is $maxCpu)")
            return false
        }
        config.put('memory', new MemoryUnit(mem))
        config.put('cpus',cpus)
        logInfo("New config: $cpus cpus and ${memStr(mem)} mem")
        return true
    }

    boolean checkTooShort(){
        try{
            def sql = new Sql(TaskDB.getDataSource())
            def searchSql = "SELECT COUNT(realtime), AVG(realtime) FROM taskrun WHERE task_name = (?)" // "and rl_active = false"
            sql.eachRow(searchSql,[taskName]) { row ->
               if (row.count && row.count as int >= 5 && row.avg && row.avg as int < 1000){
                    tooShort = true // these tasks are too short for the nextflow metrics to be accurate so we ignore them
                }
            }
            sql.close()
        } catch (SQLException sqlException) {
            log.error("There was an error: " + sqlException)
        }
        return tooShort
    }
}
