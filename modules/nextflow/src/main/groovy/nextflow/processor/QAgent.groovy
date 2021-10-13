package nextflow.processor

import groovy.sql.Sql
import java.lang.Object
import groovy.util.GroovyCollections
import groovy.util.logging.Slf4j
import nextflow.TaskDB
import nextflow.util.MemoryUnit

import java.lang.reflect.Member
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

    @Override
    String toString() {
        return "(mem $m , cpu $c)"
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
    int maxCpu
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
    boolean withLogs // TODO

    public QAgent(int initialCpu, int maxCpu, long initialMem, String taskName, boolean withLogs){
        this.chunkSize = (long) (initialMem / 4) // 5 possible memory states : -1/2 -1/4 0 +1/4 +1/2
        this.maxStateMem = 5
        this.maxMem = initialMem + 2 * chunkSize
        this.minMem = initialMem - 2 * chunkSize

        // limit our initial state space based on the original config
        if(initialCpu == 1){
            this.minCpu = 1
            this.maxCpu = Math.min(4, maxCpu)
        }
        if(initialCpu == 2){
            this.minCpu = 1
            this.maxCpu = maxCpu > 6 ? 6 : maxCpu
        }
        if(initialCpu > 2 && initialCpu <= 6){
            this.minCpu = Math.max(1,initialCpu-4)
            this.maxCpu = Math.min(initialCpu+4,maxCpu)
        }

        if(initialCpu > 6 && initialCpu <= 12){
            this.minCpu = Math.max(2,initialCpu-6)
            this.maxCpu = Math.min(initialCpu+6,maxCpu)
        }

        this.maxStateCpus = this.maxCpu - this.minCpu

        for (m in 0..<maxStateMem){
            for (c in minCpu..<maxCpu){
                def s = new State((long)((m+1)*chunkSize),c)
                def d = new StateActionData(numActions)
                states.put(s,d)
            }
        }

        this.prevState = null
        this.prevAction = -1
        this.state = new State(initialMem,initialCpu)
        this.curData = states.get(this.state)
        if(!this.curData){
            log.error("Unable to find initial state in states map! ($initialCpu cpus, $initialMem mem, statesmap = $states )")
            throw new Error("Undefined state")
        }

        //TODO what shouuld most of these be?
        this.discount = 1
        this.stepSize = 0.25
        this.epsilon = 0 // only pick a random action if we havent visited any of them before

        this.taskName = taskName
        this.lastTaskId = 0
        if(checkTooShort()){
            return
        }
        readPrevRewards()
    }

    private void readPrevRewards() {
        log.info("Searching SQL for Bandit $taskName")
        def sql = new Sql(TaskDB.getDataSource())
        def searchSql = "SELECT id,cpus,cpu_usage,realtime,memory,mem_usage FROM taskrun WHERE task_name = (?) and rl_active = true and id > (?) order by created_at asc"
        sql.eachRow(searchSql,[taskName,lastTaskId]) { row ->
            this.lastTaskId = (int) id
            def cpus = (int) row.cpus
            def cpu_usage = (float) row.cpu_usage
            def realtime = (long) row.realtime
            def mem_usage = (float) row.mem_usage
            def mem = (long) row.memory
            def r = reward(cpus,cpu_usage,realtime,mem_usage,mem)

            if(cpus != this.state.c){
                prevAction = cpus > this.state.c ? INCR_CPU : DECR_CPU
            } else if (mem != this.state.m){
                prevAction = mem > this.state.m ? INCR_MEM : DECR_MEM
            } else {
                prevAction = NOOP
            }

            def newState = new State(mem,cpus)
            def newData = states.get(newState)
            if(!newData){
                log.error("Unable to find state (c: $cpus m: $mem) in states map")
                throw new Error("Undefined state")
            }
            def qOld = curData.q[prevAction]
            double newQMax = newData.q.max()
            log.warn("maxdafuq: ${newData.q} , ${newData.q.max()} ")
            curData.q[prevAction] = qOld + stepSize * (r + discount * newQMax - qOld)

            curData.timesTried[prevAction]++
            curData.visited[prevAction] = true

            this.state = newState
            this.curData = newData

        }
        sql.close()
        log.info("Done with SQL for Bandit $taskName")
    }

    float reward(int cpu, float cpu_usage, long realtime, float mem_usage, long memAllocd) {
        //TODO: try another function?
        return -1 * realtime/1000 * (100 - cpu_usage) * (100 - mem_usage) // maximized when realtime is low and usage is high
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

    int takeNewAction(){
        if(Math.random() < epsilon) {
            // pick a random action rather than the maximizing one
            int action
            do {
                action = (int) Math.floor(Math.random() * numActions)
            } while(!actionAllowed(action))
            return action
        }

        double x = 0
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
        return aMax
    }

    public synchronized boolean takeAction(TaskConfig config){
        //TODO if too short...
        if(tooShort) {
            return false
        }
        readPrevRewards()
        action = this.takeNewAction()
        long mem = config.getMemory().getBytes()
        int cpus = config.getCpus()
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
                cpus -= 1
                break
            case INCR_CPU:
                cpus += 1
                break
            default:
                log.error("invalid action picked: $action")
                return false
        }
        if(mem < minMem || mem > maxMem ){
            log.error("Invalid value for mem: $mem")
            return false
        }
        if(cpus <= 0 || cpus > maxCpu){
            log.error("Invalid value for cpus: $cpus")
            return false
        }
        config.put('memory', new MemoryUnit(mem))
        config.put('cpus',cpus)
        return true
    }

    boolean checkTooShort(){
        try{
            def sql = new Sql(TaskDB.getDataSource())
            def searchSql = "SELECT COUNT(realtime), AVG(realtime) FROM taskrun WHERE task_name = (?)" // "and rl_active = false"
            sql.eachRow(searchSql,[taskName]) { row ->
               if (row.count && row.count as int > 5 && row.avg && row.avg as int < 1000){
                    tooShort = true // these tasks are too short for the nextflow metrics to be accurate so we ignore them
                }
            }
            sql.close()
        } catch (SQLException sqlException) {
            log.info("There was an error: " + sqlException)
        }
        return tooShort
    }
}
