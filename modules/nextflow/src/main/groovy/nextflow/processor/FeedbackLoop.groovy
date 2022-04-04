package nextflow.processor

import java.sql.SQLException
import groovy.sql.Sql
import groovy.util.logging.Slf4j
import nextflow.TaskDB
import nextflow.processor.TaskProcessor.RunType
import nextflow.util.MemoryUnit

@Slf4j
class FeedbackLoop {

    private static long toMega(m) { m >> 20 }

    private static String memPrint(long m){
        return "${toMega(m)} MB"
    }

    static boolean sizeTask(String taskName, TaskConfig config, RunType runtype, boolean withLogs) {
        log.info("Searching SQL for Task $taskName")
        def sql = new Sql(TaskDB.getDataSource())
        def searchSql = "select avg(cpu_usage) as cpu_usage, avg(peak_rss) as rss, stddev(peak_rss) as stddev , max(peak_rss) as maxmem, avg(realtime) as realtime from taskrun where task_name = (?) and rl_active = false"
        sql.eachRow(searchSql, [taskName]) { row ->
            def cpus = row.cpu_usage as int
            def mem = row.rss as long
            def maxMem = row.maxmem as long
            def memAsDouble = row.rss as double
            def stdDev = row.stddev as double
            def realtime = row.realtime as int
            withLogs && log.info("Task \"$taskName\": avg cpus ${cpus/100} -> ${Math.round(cpus/100)}, avg rss ${memPrint(mem)} (as double -> ${memPrint(memAsDouble as long)}), stddev ${stdDev} bytes, max mem ${memPrint(maxMem)} , and avg realtime $realtime")
            if(realtime < 1000){
                return false// too short for reliable metrics
            }
            def c = Math.round(cpus/100)
            long m = Math.round(mem + Math.abs(stdDev))
            long minMem = 7 << 20 // 6MB is minimum memory value for docker
            config.put("cpus", c > 0 ? c : 1)
            if (runtype == RunType.RETRY){
                log.warn("$taskName was killed with ${memPrint(Math.round(mem + Math.abs(stdDev)))} - now returning the max mem plus stddev ${memPrint(Math.round(maxMem + Math.abs(stdDev)))}")
                def oldM = config.getMemory().toBytes()
                m = Math.round(maxMem + Math.abs(stdDev))
                if(config.getErrorCount() >= 2 || m <= oldM){
                    m = Math.max(m << 1,oldM << 1)
                    m = Math.max(m,1 << 30) // some processes may need a memory argument that is at least 1 gigabyte
                    log.warn("$taskName already has 2 errors or failed with the same memory as before, now trying with double the mem or 1GB min: ${memPrint(m)}")
                }
            }
            withLogs && log.info("$taskName returning new config: ${new MemoryUnit(m > minMem ? m : minMem)} mem and cpus $c")
            config.put("memory",new MemoryUnit(m > minMem ? m : minMem))
        }
        sql.close()
        return true
    }

}
