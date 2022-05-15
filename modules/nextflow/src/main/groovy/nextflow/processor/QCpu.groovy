package nextflow.processor

import groovy.sql.Sql
import groovy.util.logging.Slf4j
import nextflow.TaskDB

import java.sql.SQLException

class CpuState {
    int c

    CpuState(int c){
        this.c = c
    }

    @Override
    String toString() {
        return "($c cpus)"
    }

    @Override
    int hashCode() {
        return c
    }

    @Override
    boolean equals(Object obj) {
        return (obj != null) && (obj instanceof CpuState) && (obj.c == this.c)
    }
}

class CpuStateActionData {
    double [] q
    boolean [] visited
    int [] timesTried

    CpuStateActionData(int numActions){
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
class QCpu {
    final int numActions = 3
    final int NOOP = 0
    final int DECR_CPU = 1
    final int INCR_CPU = 2

    CpuState state
    CpuStateActionData curData
    int maxStateCpus
    int minCpu
    int currentCpuInd
    int maxCpu
    int [] cpuOptions
    Map<CpuState,CpuStateActionData> states = [:]
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
        def date = new Date()
        f.append("(${date.toString()}) WF $command, Run name $runName, Agent \"$taskName\" ($count): $var1\n")
        log.error("Agent \"$taskName\" ($count): $var1",var2)
    }

    public QCpu(int initialCpu, String taskName, String command, String runName, boolean withLogs){
        this.withLogs = withLogs
        this.taskName = taskName
        this.count = 0
        this.command = command.split("\\./evaluation_assets/nf-core/")[1].replace('/','')
        this.runName = runName
        if(checkTooShortAndGetModulator()){
            return
        }

	    logInfo("agent config: withLogs $withLogs, command $command, initialCpu $initialCpu")

        // limit our initial state space based on the original config
        if(initialCpu == 1){
            this.minCpu = 1
            this.maxCpu = 4
            this.cpuOptions = [1, 2, 3, 4]
            this.currentCpuInd = 0
        } else if(initialCpu == 2){
            this.minCpu = 1
            this.maxCpu = 4
            this.cpuOptions = [1, 2, 4, 6]
            this.currentCpuInd = 1
        } else if(initialCpu <= 4){
            this.minCpu = 2
            this.maxCpu = 8
            this.cpuOptions = [2, 4, 6, 8]
            this.currentCpuInd = 1
        } else if(initialCpu <= 6){
            this.minCpu = 2
            this.maxCpu = 8
            this.cpuOptions = [2, 4, 6, 8, 10]
            this.currentCpuInd = 2
        } else if(initialCpu <= 8){
            this.minCpu = 4
            this.maxCpu = 10
            this.cpuOptions = [4, 6, 8, 10, 12]
            this.currentCpuInd = 2
        } else if(initialCpu > 8){
            this.minCpu = 8
            this.maxCpu = 16
            this.cpuOptions = [8, 10, 12, 14, 16]
            this.currentCpuInd = 1
        }

        this.maxStateCpus = this.cpuOptions.size()
        for (int c : this.cpuOptions){
            def s = new CpuState(c)
            def d = new CpuStateActionData(numActions)
            states.put(s,d)
        }

        this.state = new CpuState(this.cpuOptions[currentCpuInd])
        this.curData = states.get(this.state)
        if(!this.curData){
            logError("Unable to find initial state in states map! (${this.cpuOptions[this.currentCpuInd]} cpus)")
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
        def searchSql = "SELECT taskrun.id,cpus,cpu_usage,realtime,taskrun.duration,wchar FROM taskrun LEFT JOIN runs ON taskrun.run_name = runs.run_name WHERE task_name = (?) and taskrun.rl_active = true and taskrun.id > (?) and (command like (?) or taskrun.run_name = (?)) ORDER BY created_at asc"
        sql.eachRow(searchSql,[taskName, lastTaskId, "%${this.command}%".toString(), runName]) { row ->
            if (row.cpu_usage != null && row.cpus != null && (row.cpus as int) in this.cpuOptions && row.wchar) {
                this.lastTaskId = (int) row.id
                def cpus = (int) row.cpus
                def cpu_usage = (float) row.cpu_usage
                def realtime = (long) row.realtime
                def actionTaken = (int) row.wchar // dirty hack to store actions
                def r = reward(cpus, cpu_usage, realtime)

                logInfo("cpus $cpus, usage $cpu_usage, time ${realtime / 1000} seconds, avg time is ${avgRealtime / 1000} seconds (${1 - realtime / avgRealtime} speedup) -> reward $r")

                def newState = new CpuState(cpus)
                def newData = states.get(newState)
                if (!newData) {
                    logError("Unable to find data for state $newState in states map: prevState = $state")
                    throw new Error("Undefined state")
                }

                CpuState prevState
                int cpuInd = this.cpuOptions.findIndexOf({ it == cpus })
                if (cpuInd == -1) {
                    logError("couldnt find cpu allocation $cpus in ${this.cpuOptions}")
                    throw new Error("Undefined state")
                }
                switch (actionTaken) {
                    case NOOP:
                        prevState = new CpuState(cpus)
                        break;
                    case INCR_CPU:
                        prevState = new CpuState(this.cpuOptions[cpuInd - 1])
                        break;
                    case DECR_CPU:
                        prevState = new CpuState(this.cpuOptions[cpuInd + 1])
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

                if (++count > 20) {
                    epsilon = 0.25
                } else if (count > 40) {
                    epsilon = 0.1
                } else if (count > 60) {
                    epsilon = 0.02
                }
            }
        }
        sql.close()
        logInfo("Done with SQL for Agent")
    }

    float reward(int cpus, float cpu_usage, long realtime) {
        //TODO: try another function?
        return -1 * Math.max(0.1,cpus-cpu_usage/100) * realtime/avgRealtime // -1 * unused cpus *  speedup factor
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
        int c = currentCpuInd
        logInfo("Proposed action is $action for state $state and cpuInd $currentCpuInd")
        switch(action){
            case NOOP:
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
        if(cpus <= 0 || cpus < minCpu || cpus > maxCpu){
            logError("Invalid value chosen for cpus: $cpus (min cpus is $minCpu and max is $maxCpu) action was/is $action in state $state")
            return false
        }
        config.put('cpus',cpus)
        config.put('cpuAction',action) // insert into wchar
        logInfo("New config: $cpus cpus")
        this.currentCpuInd = c
        this.curData.visited[action] = true
        this.state = new CpuState(cpus)
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
