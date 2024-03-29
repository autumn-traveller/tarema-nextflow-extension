/*
 * Copyright 2020, Seqera Labs
 * Copyright 2013-2019, Centre for Genomic Regulation (CRG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nextflow.trace

import groovy.sql.Sql
import nextflow.TaskDB
import nextflow.processor.TaskRun
import nextflow.processor.TaskStatus
import org.apache.commons.codec.digest.DigestUtils

import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path

import groovy.text.GStringTemplateEngine
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.processor.TaskHandler
import nextflow.processor.TaskId
import nextflow.processor.TaskProcessor
import nextflow.script.WorkflowMetadata

import java.sql.SQLException
import java.util.concurrent.ConcurrentHashMap

/**
 * Render pipeline report processes execution.
 * Based on original TimelineObserver code by Paolo Di Tommaso
 *
 * @author Phil Ewels <phil.ewels@scilifelab.se>
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class ReportObserver implements TraceObserver {

    static final public String DEF_FILE_NAME = 'report.html'

    static final public int DEF_MAX_TASKS = 10_000

    /**
     * Holds the the start time for tasks started/submitted but not yet completed
     */
    final private Map<TaskId, TraceRecord> records = new LinkedHashMap<>()

    /**
     * Holds workflow session
     */
    private Session session

    /**
     * The path the HTML report file created
     */
    private Path reportFile

    /**
     * Max number of tasks allowed in the report, when they exceed this
     * number the tasks table is omitted
     */
    private int maxTasks = DEF_MAX_TASKS

    /**
     * Compute resources usage stats
     */
    private ResourcesAggregator aggregator

    /**
     * Overwrite existing trace file instead of rolling it
     */
    boolean overwrite

    private Map<String, Double> runMetricsMap = new ConcurrentHashMap<>();

    private Long number_tasks_executed = 0L


    /**
     * Creates a report observer
     *
     * @param file The file path where to store the resulting HTML report document
     */
    ReportObserver(Path file) {
        this.reportFile = file
    }

    /**
     * Enables the collection of the task executions metrics in order to be reported in the HTML report
     *
     * @return {@code true}
     */
    @Override
    boolean enableMetrics() {
        return true
    }

    /**
     * @return The{@link WorkflowMetadata} object associated to this execution
     */
    protected WorkflowMetadata getWorkflowMetadata() {
        session.getWorkflowMetadata()
    }

    /**
     * @return The map of collected {@link TraceRecord}s
     */
    protected Map<TaskId, TraceRecord> getRecords() {
        records
    }

    /**
     * Set the number max allowed tasks. If this number is exceed the the tasks
     * json in not included in the final report
     *
     * @param value The number of max task record allowed to be included in the HTML report
     * @return The{@link ReportObserver} itself
     */
    ReportObserver setMaxTasks(int value) {
        this.maxTasks = value
        return this
    }

    /**
     * Create the trace file, in file already existing with the same name it is
     * "rolled" to a new file
     */
    @Override
    void onFlowCreate(Session session) {
        this.session = session
        this.aggregator = new ResourcesAggregator(session)
    }

    /**
     * Save the pending processes and close the trace file
     */
    @Override
    void onFlowComplete() {
        log.debug "Flow completing -- rendering html report"
        try {
            renderHtml()
        }
        catch (Exception e) {
            log.warn "Failed to render execution report -- see the log file for details", e
        }
    }

    /**
     * This method is invoked when a process is created
     *
     * @param process A {@link TaskProcessor} object representing the process created
     */
    @Override
    void onProcessCreate(TaskProcessor process) {}


    /**
     * This method is invoked before a process run is going to be submitted
     *
     * @param handler A {@link TaskHandler} object representing the task submitted
     */
    @Override
    void onProcessSubmit(TaskHandler handler, TraceRecord trace) {
        log.trace "Trace report - submit process > $handler"
        synchronized (records) {
            records[trace.taskId] = trace
        }
    }

    /**
     * This method is invoked when a process run is going to start
     *
     * @param handler A {@link TaskHandler} object representing the task started
     */
    @Override
    void onProcessStart(TaskHandler handler, TraceRecord trace) {
        log.trace "Trace report - start process > $handler"
        synchronized (records) {
            records[trace.taskId] = trace
        }
    }

    /**
     * This method is invoked when a process run completes
     *
     * @param handler A {@link TaskHandler} object representing the task completed
     */
    @Override
    void onProcessComplete(TaskHandler handler, TraceRecord trace) {
        log.info "Trace report - complete process > $handler"
        if (!trace) {
            log.debug "WARN: Unable to find trace record for task id=${handler.task?.id}"
            return
        }

        // ÜBERPRÜFEN, ob TASK success
        if (!(trace.getProperty("status") as String).equals("FAILED")) {
            sendDataToDB(handler.getTask(), trace)
        } else {
            if(trace.get('exit') as int in [1,137,138,139,140]){ // 1 and 137-140 are usually memory errors
                log.warn("think we have an oom victim: $trace, rtime: ${trace.get("realtime")}")
                sendDataToDB(handler.getTask(), trace)
            } else {
                log.warn("Report Observer non memory error (${trace.get('exit')}): $trace")
                def task = handler.getTask()
                if (task.error.message?.contains("exceeded running time") ) {
                    log.warn('runtime exceeded, storing data as well')
                    sendDataToDB(task, trace)
                }
            }
        }

        synchronized (records) {
            records[trace.taskId] = trace
            aggregate(trace)
        }
    }

    private BigDecimal calcNewAvg(String key, Double current_value) {
        runMetricsMap.put(key, (Double) (runMetricsMap.get(key) + (1 / number_tasks_executed) * (current_value - runMetricsMap.get(key))))
    }


    private void sendDataToDB(TaskRun task, TraceRecord trace) {

        if (task.getConfig().get("bandit") != "active" && task.getConfig().get("vanilla") != "active"){
            return
        }

        def sql = new Sql(TaskDB.getDataSource())
        def taskName = task.name.split(" ")[0]

        def insertSql,params
        if(trace.get('exit') as int in 137..140 || !trace.get('peak_rss')){
            // killed by oom killer
            log.info("Task $taskName killed by oom killer")
            insertSql = 'INSERT INTO TaskRun (task_name, peak_rss, cpus, memory, realtime, rl_active, run_name, wf_name, duration, rchar, wchar) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
            params = [taskName, task.getConfig().getMemory().getBytes()+1, task.getConfig().getCpus(), task.getConfig().getMemory().getBytes(), trace.get("realtime"), session.config.withLearning, session.runName, task.container, task.getConfig().get("memAction"), task.getConfig().get("prevMemState"),task.getConfig().get("cpuAction")]

//        } else if (task.error.message?.contains("exceeded running time")) {
//            // how should we handle this case ?
        } else {

            insertSql = 'INSERT INTO TaskRun (task_name, cpu_usage, rss, peak_rss, vmem, peak_vmem, rchar, wchar, cpus, memory, realtime, duration, rl_active, run_name, wf_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
            params = [taskName, trace.get("%cpu"), trace.get("rss"), trace.get("peak_rss"), trace.get("vmem"), trace.get("peak_vmem"), task.getConfig().get("prevMemState"), task.getConfig().get("cpuAction"), task.getConfig().getCpus(), task.getConfig().getMemory().getBytes(), trace.get("realtime"), task.getConfig().get("memAction"), session.config.withLearning, session.runName, task.container]
            number_tasks_executed++
            if (runMetricsMap.size() < 5) {
                runMetricsMap.put("cpu", Double.valueOf(trace.get("%cpu") as String))
                runMetricsMap.put("rss", Double.valueOf(trace.get("rss") as String))
                runMetricsMap.put("rchar", Double.valueOf(trace.get("rchar") as String))
                runMetricsMap.put("wchar", Double.valueOf(trace.get("wchar") as String))
                runMetricsMap.put("realtime", Double.valueOf(trace.get("realtime") as String))

            } else {
                calcNewAvg("cpu", Double.valueOf(trace.get("%cpu") as String))
                calcNewAvg("rss", Double.valueOf(trace.get("rss") as String))
                calcNewAvg("rchar", Double.valueOf(trace.get("rchar") as String))
                calcNewAvg("wchar", Double.valueOf(trace.get("wchar") as String))
                calcNewAvg("realtime", Double.valueOf(trace.get("realtime") as String))
            }
        }

        try {
            sql.executeInsert(insertSql, params)
            log.info("Successfully inserted $taskName")
        } catch (SQLException sqlException) {
            log.info("There was an error: " + sqlException)
        }


        sql.close()

        // TODO hier müssen die Sachen + der Trace abgespeichert werden
        // Wenn der Task neu ist -> einfach rein, wenn dir den Task bereits hatten, ist dies Exploration. D.h wir müssen unser Model über diesen Task updaten.
        // Wir brauchen also extra table für die Models der einzelnen Tasks
        // Wir müssen den Task einschätzen -> klassifizieren mit dem Model


        // Bevor ein Task laufen gelassen wird, müssen wir die DB checken, ob der Task bereits lief
        // dann z.B nach 0,1 epsillon eine neue Config wählen

    }

    /**
     * This method is invoked when a process run cache is hit
     *
     * @param handler A {@link TaskHandler} object representing the task cached
     */
    @Override
    void onProcessCached(TaskHandler handler, TraceRecord trace) {
        log.trace "Trace report - cached process > $handler"

        // event was triggered by a stored task, ignore it
        if (trace == null) {
            return
        }

        // remove the record from the current records
        synchronized (records) {
            records[trace.taskId] = trace
            aggregate(trace)
        }
    }

    /**
     * Aggregates task record for each process in order to render the
     * final execution stats
     *
     * @param record A {@link TraceRecord} object representing a task executed
     */
    protected void aggregate(TraceRecord record) {
        aggregator.aggregate(record)
    }

    /**
     * @return The tasks json payload
     */
    protected String renderTasksJson() {
        final r = getRecords()


        /*
        for (Map.Entry<TaskId,TraceRecord> entry : r.entrySet()) {
            log.debug(entry.getKey() + "/" + entry.getValue());
            log.debug(entry.getValue().get("rss").toString())

            HashMap<String,AttributeValue> item_key =
                    new HashMap<String,AttributeValue>();

            HashMap<String,AttributeValueUpdate> updated_values =
                    new HashMap<String, AttributeValueUpdate>();

            updated_values.put("memory", new AttributeValueUpdate(
                    new AttributeValue(entry.getValue().get("rss").toString()), AttributeAction.PUT));

            updated_values.put("read_bytes", new AttributeValueUpdate(
                    new AttributeValue(entry.getValue().get("read_bytes").toString()), AttributeAction.PUT));

            item_key.put("id", new AttributeValue(entry.getKey().toString()));

            ddb.updateItem("Task",item_key, updated_values );
        }

        */
        // TODO


        r.size() <= maxTasks ? renderJsonData(r.values()) : 'null'
    }

    protected String renderSummaryJson() {
        final result = aggregator.renderSummaryJson()
        log.debug "Execution report summary data:\n  ${result}"
        return result
    }

    protected String renderPayloadJson() {
        "{ \"trace\":${renderTasksJson()}, \"summary\":${renderSummaryJson()} }"
    }

    /**
     * Render the report HTML document
     */
    protected void renderHtml() {

        // render HTML report template
        final tpl_fields = [
                workflow  : getWorkflowMetadata(),
                payload   : renderPayloadJson(),
                assets_css: [
                        readTemplate('assets/bootstrap.min.css'),
                        readTemplate('assets/datatables.min.css')
                ],
                assets_js : [
                        readTemplate('assets/jquery-3.2.1.min.js'),
                        readTemplate('assets/popper.min.js'),
                        readTemplate('assets/bootstrap.min.js'),
                        readTemplate('assets/datatables.min.js'),
                        readTemplate('assets/moment.min.js'),
                        readTemplate('assets/plotly.min.js'),
                        readTemplate('assets/ReportTemplate.js')
                ]
        ]
        final tpl = readTemplate('ReportTemplate.html')
        def engine = new GStringTemplateEngine()
        def html_template = engine.createTemplate(tpl)
        def html_output = html_template.make(tpl_fields).toString()

        // make sure the parent path exists
        def parent = reportFile.getParent()
        if (parent)
            Files.createDirectories(parent)

        if (overwrite)
            Files.deleteIfExists(reportFile)
        else
        // roll the any trace files that may exist
            reportFile.rollFile()

        def writer = Files.newBufferedWriter(reportFile, Charset.defaultCharset())
        writer.withWriter { w -> w << html_output }
        writer.close()
    }

    /**
     * Render the executed tasks json payload
     *
     * @param data A collection of {@link TraceRecord}s representing the tasks executed
     * @return The rendered json payload
     */
    protected String renderJsonData(Collection<TraceRecord> data) {
        def List<String> formats = null
        def List<String> fields = null
        def result = new StringBuilder()
        result << '[\n'
        int i = 0
        for (TraceRecord record : data) {
            if (i++) result << ','
            if (!formats) formats = TraceRecord.FIELDS.values().collect { it != 'str' ? 'num' : 'str' }
            if (!fields) fields = TraceRecord.FIELDS.keySet() as List
            record.renderJson(result, fields, formats)
        }
        result << ']'
        return result.toString()
    }

    /**
     * Read the document HTML template from the application classpath
     *
     * @param path A resource path location
     * @return The loaded template as a string
     */
    private String readTemplate(String path) {
        StringWriter writer = new StringWriter();
        def res = this.class.getResourceAsStream(path)
        int ch
        while ((ch = res.read()) != -1) {
            writer.append(ch as char);
        }
        writer.toString();
    }

}
