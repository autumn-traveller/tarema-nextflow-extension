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

    static void sizeTask(String taskName, TaskConfig config, RunType runtype, boolean withLogs) {
        logInfo("Searching SQL for Task $taskName")
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
                return // too short for reliable metrics
            }
            def c = Math.round(cpus/100)
            def m = Math.round(mem + Math.abs(stdDev))
            def minMem = 7 << 20 // 6MB is minimum memory value for docker
            config.put("cpus", c > 0 ? c : 1)
            if (runtype == RunType.RETRY){
                log.warn("$taskName was killed with ${memPrint(Math.round(mem + Math.abs(stdDev)))} - now returning the max mem plus stddev ${memPrint(Math.round(maxMem + Math.abs(stdDev)))}")
                m = Math.round(maxMem + Math.abs(stdDev))
            }
            config.put("memory",new MemoryUnit(m > minMem ? m : minMem))
        }
        sql.close()

    }

}
