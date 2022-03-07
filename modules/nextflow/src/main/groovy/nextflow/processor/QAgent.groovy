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
    String command
    String runName
    float epsilon
    int lastTaskId
    boolean tooShort = false
    boolean withLogs
    int count
    long avgRealtime

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

    public QAgent(int initialCpu, int maxCpu, long initialMem, long maxSystemMem, String taskName, String command, String runName, boolean withLogs){
        this.withLogs = withLogs
        this.taskName = taskName
        this.count = 0
        this.command = command
        this.runName = runName
        if(checkTooShortAndGetModulator()){
            return
        }

        if(initialMem + initialMem/2 >= maxSystemMem){
            this.chunkSize = initialMem >> 2 // 3 possible memory states : -1/2 -1/4 0
            this.maxStateMem = 3
            this.maxMem = initialMem
            this.minMem = initialMem - 2*chunkSize
            initialMem -= chunkSize // start in the middle state (-1/4)
        } else {
//        this.chunkSize = initialMem >> 2 // 5 possible memory states : -1/2 -1/4 0 +1/4 +1/2
            this.chunkSize = initialMem >> 1 // 3 possible memory states : -1/2 0 +1/2
//        this.maxStateMem = 5
            this.maxStateMem = 3
//        this.maxMem = initialMem + 2 * chunkSize
            this.maxMem = initialMem + chunkSize
//        this.minMem = initialMem - 2 * chunkSize
            this.minMem = initialMem - chunkSize
        }

	logInfo("agent config: withLogs $withLogs, command $command, initialCpu $initialCpu, initialMem ${memStr(initialMem)}")
        logInfo("Memory chunksize : ${memStr(chunkSize)} min: $minMem (${memStr(minMem)}) and max $maxMem (${memStr(maxMem)})}")

        // limit our initial state space based on the original config
        if(initialCpu == 1){
            this.minCpu = 1
            this.maxCpu = 4
            this.cpuOptions = [1, 2, 4]
            this.currentCpuInd = 0
        } else if(initialCpu == 2){
            this.minCpu = 1
            this.maxCpu = 4
            this.cpuOptions = [1, 2, 4]
            this.currentCpuInd = 1
        } else if(initialCpu <= 4){
            this.minCpu = 2
            this.maxCpu = 8
            this.cpuOptions = [2, 4, 6, 8]
            this.currentCpuInd = 1
        } else if(initialCpu <= 6){
            this.minCpu = 2
            this.maxCpu = 8
            this.cpuOptions = [2, 4, 6, 8]
            this.currentCpuInd = 2
        } else if(initialCpu <= 8){
            this.minCpu = 4
            this.maxCpu = 10
            this.cpuOptions = [4, 6, 8, 10]
            this.currentCpuInd = 2
        } else if(initialCpu > 8){
            this.minCpu = 8
            this.maxCpu = 16
            this.cpuOptions = [8, 12, 16]
            this.currentCpuInd = 1
        }

        this.maxStateCpus = this.cpuOptions.size()
        String str = ""
        for (m in 0..<maxStateMem){
            str += "Mem option $m: ${memStr(initialMem + (m-1)*chunkSize)} "
            //str += "Mem option $m: ${memStr(initialMem + (m-2)*chunkSize)} "
            for (int c : this.cpuOptions){
                def s = new State((long)(initialMem + (m-1)*chunkSize),c)
                // def s = new State((long)(initialMem + (m-2)*chunkSize),c)
                def d = new StateActionData(numActions)
                states.put(s,d)
            }
        }
        logInfo(str)
        logInfo('ALL STATES',states)

        this.state = new State(initialMem,this.cpuOptions[currentCpuInd])
        this.curData = states.get(this.state)
        if(!this.curData){
            logError("Unable to find initial state in states map! (${this.cpuOptions[this.currentCpuInd]} cpus, $initialMem mem, statesmap = $states )")
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
        def searchSql = "SELECT taskrun.id,cpus,cpu_usage,realtime,memory,peak_rss,taskrun.duration FROM taskrun LEFT JOIN runs ON taskrun.run_name = runs.run_name WHERE task_name = (?) and taskrun.rl_active = true and taskrun.id > (?) and (command = (?) or taskrun.run_name = (?)) ORDER BY created_at asc"
        sql.eachRow(searchSql,[taskName, lastTaskId, command, runName]) { row ->
            this.lastTaskId = (int) row.id
            def cpus = (int) row.cpus
            def cpu_usage = (float) row.cpu_usage
            def realtime = (long) row.realtime
            def rss =  (long) row.peak_rss
            def mem = (long) row.memory
            double mem_usage = ((double) rss)/((double) mem)
            def actionTaken = (int) row.duration // dirty hack to store actions for now
            def r = reward(cpus,cpu_usage,realtime,mem_usage,mem,rss)

            logInfo("cpus $cpus, usage $cpu_usage, time ${realtime/1000} seconds, avg time is ${avgRealtime/1000} seconds (${1-realtime/avgRealtime} speedup), mem ${memStr(mem)}, rss ${memStr(rss)} (mem_usage ${mem_usage*100}%) -> reward $r")

            def newState = new State(mem,cpus)
            def newData = states.get(newState)
            if(!newData){
                logError("Unable to find data for state $newState in states map: prevState = $state")
                throw new Error("Undefined state")
            }

            State prevState
            int cpuInd = this.cpuOptions.findIndexOf({ it == cpus })
            if(cpuInd == -1){
                logError("couldnt find cpu allocation $cpus in ${this.cpuOptions}")
                throw new Error("Undefined state")
            }
            switch(actionTaken){
                case NOOP:
                    prevState = new State(mem,cpus)
                    break;
                case INCR_MEM:
                    prevState = new State(mem-chunkSize,cpus)
                    break;
                case DECR_MEM:
                    prevState = new State(mem+chunkSize,cpus)
                    break;
                case INCR_CPU:
                    prevState = new State(mem,this.cpuOptions[cpuInd-1])
                    break;
                case DECR_CPU:
                    prevState = new State(mem,this.cpuOptions[cpuInd+1])
                    break;
                default:
                    logError("Bad action in DB $actionTaken")
                    throw new Error("Undefined state")
                    break;
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
            this.currentCpuInd = cpuInd
            this.curData = newData

            if(++count > 50){
                epsilon = 0.25
            } else if(count > 100){
                epsilon = 0.1
            } else if (count > 150){
                epsilon = 0.02
            }

        }
        sql.close()
        logInfo("Done with SQL for Agent")
    }

    float reward(int cpus, float cpu_usage, long realtime, double mem_usage, long memAllocd, long rss) {
        //TODO: try another function?
//        return cpus*5 + Math.min(90f,cpu_usage/cpus) + Math.min(65f,mem_usage) // maximized when resource usage is high
        // return -1 * Math.sqrt(Math.abs(cpus - cpu_usage/cpus)) * realtime/modulator * ((memAllocd >> 20) * (1-mem_usage))
//        return Math.min(85.0,cpu_usage/cpus) + 100*(1 - realtime/avgRealtime) + Math.min(70.0,mem_usage*100) // cpu use + speedup + mem use with caps
        return -1 * Math.max(0.1,cpus-cpu_usage/100) * realtime/avgRealtime * ((memAllocd >> 20) * (1 - Math.max(0.75,mem_usage))) // -1 * unused cpus *  speedup factor * mis-allocated mem
    }

    void logBandit(){

    }

    boolean actionAllowed(int a){
        if(a == DECR_CPU && state.c == minCpu){
            curData.q[a] = Double.NEGATIVE_INFINITY // dont accidentally pick this action as qMax
            return false
        }
        if(a == INCR_CPU && state.c == maxCpu){
            curData.q[a] = Double.NEGATIVE_INFINITY
            return false
        }
        if(a == DECR_MEM && state.m == minMem){
            curData.q[a] = Double.NEGATIVE_INFINITY
            return false
        }
        if(a == INCR_MEM && state.m == maxMem){
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
        long mem = this.state.m
        int c = currentCpuInd
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
            case DECR_CPU:
                c -= 1
                break
            case INCR_CPU:
                c += 1
                break
            default:
                logError("invalid action picked: $action in state $state")
                return false
        }
        int cpus = cpuOptions[c]
        if(mem < minMem || mem > maxMem ){
            logError("Invalid value chosen for mem: ${memStr(mem)} (chunk size is ${memStr(chunkSize)}) and min is ${memStr(minMem)} and max is ${memStr(maxMem)} action was/is $action in state $state")
            return false
        }
        if(cpus <= 0 || cpus < minCpu || cpus > maxCpu){
            logError("Invalid value chosen for cpus: $cpus (min cpus is $minCpu and max is $maxCpu) action was/is $action in state $state")
            return false
        }
        config.put('memory', new MemoryUnit(mem))
        config.put('cpus',cpus)
        config.put('action',action)
        logInfo("New config: $cpus cpus and ${memStr(mem)} mem")
        this.currentCpuInd = c
        this.curData.visited[action] = true
        this.state = new State(mem,cpus)
        return true
    }

    boolean checkTooShortAndGetModulator(){
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
